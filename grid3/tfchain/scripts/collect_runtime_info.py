import grid3.tfchain

tfchain = grid3.tfchain.TFChain()
last_version = None

for i in range(0, 20_000_000, 100_000):
    try:
        version = tfchain.sub.get_block_runtime_version(tfchain.sub.get_block_hash(i))[
            "specVersion"
        ]
        print(i, version)
        if last_version is None:
            last_version = version
        elif version != last_version:
            tfchain.sub.init_runtime(block_id=i)
            tfchain.sub.implements_scaleinfo()
            print(
                f"Runtime version changed at block {i} from {last_version} to {version}; ScaleInfo: {tfchain.sub.implements_scaleinfo()}"
            )
            last_version = version
    except Exception as e:
        print(f"Error at block {i}: {e}")
