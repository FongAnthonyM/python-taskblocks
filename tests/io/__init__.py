#!/usr/bin/env python3
# countasync.py

import asyncio

async def count():
    print("One")
    await asyncio.sleep(1)
    print("Two")

def test():
    print(asyncio.events._get_running_loop())
    await asyncio.gather(count(), count(), count())

async def main():
    test()

if __name__ == "__main__":
    import time

    print(asyncio.events._get_running_loop())

    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")
