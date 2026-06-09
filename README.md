# Anyio-Executor
Inspired by the newer [aiolibs-executor](https://github.com/aio-libs/aiolibs-executor) library
this library provides a asynchronous executor for anyio and has compatability
with 3.10 and newer versions of python.


## Installation
```
pip install anyio-executor
```

## Usage

It borrows a simillar interface to that of [aiothreading](https://github.com/Vizonex/aiothreading)
however this library is a lot more simplistic as an external thread is not being launched. 
As for queues, anyio doesn't support having it's own queue objects so in order to meet demands for 
excellent performance [aiologic](https://github.com/x42005e1f/aiologic) is utilized for that role.

```python
import anyio

from anyio_executor import Executor


async def fn(i: int):
    await anyio.sleep(i)
    print(f"{i} done!")
    return i

async def main():
    async with Executor(10) as e:
        items = await e.map(fn, [1, 2, 1, 2, 3])
    print(items)

    async with Executor(10) as e:
        # it will also take async for iterations.
        async for i in e.map(fn, [1, 2, 1, 2, 3]):
            print(i)

if __name__ == "__main__":
    anyio.run(main)
```
