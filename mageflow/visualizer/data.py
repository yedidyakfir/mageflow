import asyncio

import rapyer

import mageflow
from mageflow.signature.model import TaskSignature


async def extract_signatures() -> list[TaskSignature]:
    redis_models = rapyer.find_redis_models()
    signature_models = [
        klass for klass in redis_models if issubclass(klass, TaskSignature)
    ]
    # TODO - use large get action once we add this
    signatures_key = await asyncio.gather(
        *[klass.afind_keys() for klass in signature_models]
    )
    sigantures = await asyncio.gather(
        *[rapyer.get(key) for keys in signatures_key for key in keys]
    )
    return sigantures


async def create_chain(name: str):
    sign1_callback = await mageflow.sign(f"sign1_callback-{name}")
    sign2_callback = await mageflow.sign(f"sign2_callback-{name}")
    sign3_callback = await mageflow.sign(f"sign3_callback-{name}")
    sign4_callback = await mageflow.sign(f"sign4_callback-{name}")
    sign1 = await mageflow.sign(
        "task1", success_callbacks=[sign1_callback, sign2_callback]
    )
    sign2 = await mageflow.sign("task2")
    sign3 = await mageflow.sign(
        "task3", error_callbacks=[sign3_callback], success_callbacks=[sign4_callback]
    )
    sign4 = await mageflow.sign("task4")
    sign5 = await mageflow.sign("task5")

    another_sign = await mageflow.sign("another_task")
    callback_sign = await mageflow.sign(
        f"callback-{name}",
        error_callbacks=[another_sign],
    )
    chain_sign = await mageflow.chain(
        [sign1, sign2, sign3, sign4, sign5], error=callback_sign
    )
    return chain_sign


async def create(redis_url: str):
    await rapyer.init_rapyer(redis_url)
    chain_task1 = await create_chain("chain1")
    chain_task2 = await create_chain("chain2")
    chain_task3 = await create_chain("chain3")
    chain_task4 = await create_chain("chain4")
    swarm_task = await mageflow.swarm(
        tasks=[chain_task1, chain_task2, chain_task3], success_callbacks=[chain_task4]
    )
    chain_task1 = await create_chain("chain5")
    chain_task2 = await create_chain("chain6")
    chain_task3 = await create_chain("chain7")
    chain_task4 = await create_chain("chain8")
    swarm_task = await mageflow.swarm(
        tasks=[chain_task1, chain_task2, chain_task3], error_callbacks=[chain_task4]
    )
