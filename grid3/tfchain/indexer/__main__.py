import argparse
import datetime
import time
from multiprocessing import Process
from threading import Thread

import prometheus_client
from websocket._exceptions import (
    WebSocketAddressException,
    WebSocketConnectionClosedException,
)

from .. import tfchain
from ..minting.period import Period
from .archiver import Archiver
from .lib import Indexer

MIN_WORKERS = 2
SLEEP_TIME = 30
DB_TIMEOUT = 30
POST_PERIOD = 60 * 60

# When querying a fixed period of blocks, how many times to retry missed blocks
RETRIES = 3

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--file",
        help="Specify the database file name.",
        type=str,
        default="tfchain.db",
    )
    parser.add_argument(
        "-s",
        "--start",
        help="Give a timestamp to start scanning blocks. If omitted, scanning starts from beginning of current minting period",
        type=int,
    )
    parser.add_argument(
        "--start-block", help="Give a block number to start scanning blocks", type=int
    )
    parser.add_argument(
        "-e",
        "--end",
        help="By default, scanning continues to process new blocks as they are generated. When an end timestamp is given, scanning stops at that block height and the program exits",
        type=int,
    )
    parser.add_argument(
        "--end-block",
        help="Specify end by block number rather than timestamp",
        type=int,
    )
    parser.add_argument(
        "-m",
        "--max-workers",
        help="Maximum number of worker processes to spawn",
        type=int,
        default=50,
    )
    parser.add_argument(
        "--archive",
        help="Enable archive mode: store blocks as compressed JSON in batches of 10",
        action="store_true",
    )

    args = parser.parse_args()

    print("Staring up, preparing to ingest some blocks, nom nom")

    # Create an instance of the Indexer class
    indexer = Indexer(
        file=args.file,
        db_timeout=DB_TIMEOUT,
        max_workers=args.max_workers,
        min_workers=MIN_WORKERS,
        sleep_time=SLEEP_TIME,
        post_period=POST_PERIOD,
        retries=RETRIES,
        archive_mode=args.archive,
    )

    # Initialize archive compressor if archive mode is enabled
    archiver = None
    if args.archive:
        print("Archive mode enabled")
        archiver = Archiver(args.file)

    # Prep database and grab already processed blocks
    con = indexer.new_connection()
    indexer.prep_db(con)

    # Prepare archive database if in archive mode
    if args.archive:
        archiver.prepare_db_for_archive(con)
        # Try to load existing compression dictionary
        if not archiver.load_dict_from_db(con):
            print("No existing compression dictionary found, will train a new one")

    # Start tfchain client
    client = tfchain.TFChain()

    # Train compression dictionary if needed (archive mode only)
    if args.archive and archiver.zstd_dict is None:
        print("Training compression dictionary with 1000 random blocks...")
        sample_blocks = archiver.sample_random_blocks(client, count=1000)
        zstd_dict = archiver.train_compression_dict(sample_blocks)
        archiver.zstd_dict = zstd_dict
        archiver.save_dict_to_db(con, zstd_dict)
        print("Compression dictionary trained and saved")

    if args.start_block is not None:
        start_number = args.start_block
    elif args.start:
        start_number = client.find_block_minting(args.start)
    else:
        # By default, use beginning of current minting period
        start_number = client.find_block_minting(Period().start)

    writer_proc = Process(target=indexer.db_writer)
    writer_proc.daemon = True
    writer_proc.start()

    powers_thread = Thread(target=indexer.fetch_powers, args=[start_number])
    powers_thread.daemon = True
    powers_thread.start()

    if args.end or args.end_block:
        if args.end_block:
            end_number = args.end_block
        else:
            end_number = client.find_block_minting(args.end + POST_PERIOD)

        processes = indexer.parallelize(con, start_number, end_number)

        while (block_qsize := indexer.block_queue.qsize()) > 0:
            time.sleep(SLEEP_TIME)
            processes = [t for t in processes if t.is_alive()]
            print(
                datetime.datetime.now(),
                "processed",
                block_qsize - indexer.block_queue.qsize(),
                "blocks in",
                SLEEP_TIME,
                "seconds",
                indexer.block_queue.qsize(),
                "blocks remaining",
                len(processes),
                "processes alive",
                indexer.write_queue.qsize(),
                "write jobs",
            )
            indexer.scale_workers(processes)

        print("Joining blocks queue")
        indexer.block_queue.join()
        print("Joining write queue")
        indexer.write_queue.join()
        # Retry any missed blocks three times. Since we don't handle errors in the when fetching and processing blocks, it's normal to miss a few
        while missing_count := indexer.load_queue(con, start_number, end_number):
            print(
                datetime.datetime.now(),
                missing_count,
                "blocks to retry",
                len(processes),
                "processes alive",
            )
            indexer.block_queue.join()
            indexer.write_queue.join()

        # Finally wait for any remaining jobs to complete
        indexer.block_queue.join()
        indexer.write_queue.join()
        # Signal remaining processes to exit
        [indexer.block_queue.put(-1) for p in processes if p.is_alive()]
        indexer.write_queue.put(None)

    else:
        # This is the case where we continue running and fetch all new blocks as they are generated

        # Prep Prometheus instrumentation. We only use this in long running mode
        prometheus_client.start_http_server(8000)
        blocks_counter = prometheus_client.Counter(
            "blocks_processed", "Counts how many blocks have processed successfully"
        )
        blocks_gauge = prometheus_client.Gauge(
            "block_number", "Highest block number processed so far"
        )
        block_queue_gauge = prometheus_client.Gauge(
            "block_queue_length", "How many blocks are queued to be processed"
        )
        write_queue_gauge = prometheus_client.Gauge(
            "write_queue_length", "Current number of items in write queue"
        )

        # Since using the subscribe method blocks, we give it a thread
        sub_thread = indexer.spawn_subscriber(client)

        # We wait to get the first block number back from the subscribe callback, so that we're sure which block is the end of the historic range we want to queue up
        block_number = indexer.block_queue.get()
        indexer.block_queue.put(block_number)
        processes = indexer.parallelize(
            con,
            start_number,
            block_number - 1,
        )

        current_period = Period()
        processed_count = con.execute(
            "SELECT COUNT(1) FROM processed_blocks"
        ).fetchone()[0]

        checkpoint_block = con.execute(
            "SELECT value FROM kv WHERE key='checkpoint_block'"
        ).fetchone()[0]
        if checkpoint_block == 0:
            con.execute(
                "UPDATE kv SET value=? WHERE key='checkpoint_block'", (start_number,)
            )
            con.commit()

        while 1:
            time.sleep(SLEEP_TIME)

            # We can periodically get disconnected from the websocket. On each loop we try once to reconnect in case we need the client below
            if not client.sub.websocket.connected:
                try:
                    client.sub.connect_websocket()
                except WebSocketAddressException as e:
                    print(e)

            # We just discard any processes that have died for any reason. They will be replaced by the auto scaling. In fact, we don't try to handle errors at all in the worker processes--the blocks just get retried later
            processes = [t for t in processes if t.is_alive()]
            new_count = con.execute("SELECT COUNT(1) FROM processed_blocks").fetchone()[
                0
            ]
            processed_this_period = new_count - processed_count
            print(
                "{} processed {} blocks in {} seconds {} blocks queued {} processes alive {} write jobs".format(
                    datetime.datetime.now(),
                    processed_this_period,
                    SLEEP_TIME,
                    indexer.block_queue.qsize(),
                    len(processes),
                    indexer.write_queue.qsize(),
                )
            )
            if indexer.block_queue.qsize() > 500:
                # Estimate time remaining based on recent processing rate
                rate = processed_this_period / SLEEP_TIME
                if rate > 0:
                    remaining_seconds = indexer.block_queue.qsize() / rate
                    remaining_minutes = int(remaining_seconds / 60)
                    remaining_hours = remaining_minutes // 60
                    remaining_minutes = remaining_minutes % 60
                    print(
                        "Estimated time remaining: {} hours {} minutes".format(
                            remaining_hours, remaining_minutes
                        )
                    )

            processed_count = new_count

            blocks_counter.inc(processed_this_period)
            write_queue_gauge.set(indexer.write_queue.qsize())
            block_queue_gauge.set(indexer.block_queue.qsize())
            blocks_gauge.set(
                con.execute(
                    "SELECT MAX(block_number) FROM processed_blocks"
                ).fetchone()[0]
            )

            # Check for missing blocks only when the queue is cleared, to avoid placing duplicate entries in the queue. In theory it's possible the queue never empties due to bad conditions, but in practice the resting state is an empty block queue
            # We record the max block for which we have processed all preceding blocks as a "checkpoint" and also the timestamp. This helps keep this computation in check as the size of processed blocks grows. We'll also use the checkpoint timestamps when searching for violations, to see if block processing has fallen behind
            if indexer.block_queue.qsize() == 0:
                first_block = con.execute(
                    "SELECT value FROM kv WHERE key='checkpoint_block'"
                ).fetchone()[0]
                print("Block checkpoint is:", first_block)
                print(
                    "Block checkpoint time is:",
                    con.execute(
                        "SELECT value FROM kv WHERE key='checkpoint_time'"
                    ).fetchone()[0],
                )

                last_block = con.execute(
                    "SELECT MAX(block_number) FROM processed_blocks"
                ).fetchone()[0]
                print("Last processed block is:", last_block)
                missing_blocks = indexer.find_missing(con, first_block, last_block)

                if missing_blocks:
                    for b in missing_blocks:
                        indexer.block_queue.put(b)
                    print("Queued", len(missing_blocks), "missing blocks")
                else:
                    # TODO: Ideally we would store the timestamps of the blocks as they are processed initially rather than querying for it again
                    try:
                        block = client.sub.get_block(block_number=last_block)
                        timestamp = client.get_timestamp(block) // 1000
                        with con:
                            con.execute(
                                "UPDATE kv SET value=? WHERE key='checkpoint_block'",
                                (last_block,),
                            )
                            con.execute(
                                "UPDATE kv SET value=? WHERE key='checkpoint_time'",
                                (timestamp,),
                            )
                    except WebSocketConnectionClosedException as e:
                        # We already try reconnecting on each pass of the loop, so here just log the error and move on
                        print(e)

            indexer.scale_workers(processes)

            # If we have entered a new minting period, spawn a thread to fetch the power info for each node at the start of the new period
            period = Period()
            if period.offset > current_period.offset:
                start_number = client.find_block_minting(period.start)
                powers_thread = Thread(target=indexer.fetch_powers, args=[start_number])
                powers_thread.daemon = True
                powers_thread.start()
                current_period = period

            # Also make sure we keep alive our subscription thread. If there's an error in the callback, it propagates up and the thread dies
            if not sub_thread.is_alive():
                print("Subscription thread died, respawning it")
                sub_thread = indexer.spawn_subscriber(client)

            if not writer_proc.is_alive():
                print("Writer proc died, respawning it")
                writer_proc = Process(target=indexer.db_writer)
                writer_proc.daemon = True
                writer_proc.start()
