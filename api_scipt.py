import os
import sys
from tqdm import tqdm
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(os.path.join(current_dir, "openai_rpc"))

import re
import json
import uuid
import asyncio
import random
from concurrent.futures import ThreadPoolExecutor

import aiofiles
import aiolimiter
from tqdm.asyncio import tqdm
from google.protobuf.json_format import MessageToJson
from kess.framework import (
    ClientOption,
    GrpcClient,
)

from mmu.mmu_chat_gpt_pb2 import MmuChatGptRequest, MmuChatGptResponse
from mmu.mmu_chat_gpt_pb2_grpc import (
    MmuChatGptServiceStub
)
biz_dict = {
    "gpt-35-turbo": "liuchenxiao_fd77a4cc_gpt-35-turbo-1106",
    "gpt-4": "liuchenxiao_fd77a4cc_gpt-4-0613",
    "gpt-4-32k": "liuchenxiao_fd77a4cc_gpt-4-32k-0613"
}

def build_prompt(entry, prompt_template):
    olympics_name = entry["olympics_name"]
    olympics_knowledge = entry["olympics_knowledge"]
    prompt = prompt_template.strip().replace("{olympics_name}", olympics_name).replace("{olympics_knowledge}", olympics_knowledge)
    return prompt


def fetch_result(client, request, timeout=None):
    resp = client.Chat(request, timeout=timeout)
    return resp


def parse_result(entry, result):
    answer = result["answer"]
    answer = answer.strip()
    templates = []
    for line in answer.split("\n"):
        line = line.strip()
        # line = re.sub(r'\(.*?\)|（.*?）', '', line)
        if line.startswith("- "):
            line = line[2:].strip()
            # line = line.strip('“"”')
            templates.append(line)

    if len(templates) == 0:
        return None

    return {
        "question": templates,
        "reference": entry["olympics_knowledge"]
    }


async def run_generator(
        client, biz_key, config, prompt, limiter: aiolimiter.AsyncLimiter, timeout=90, max_retries=3
):
    attempts = 0
    while attempts < max_retries:
        async with limiter:
            try:
                biz = biz_dict[biz_key]
                session_id = str(uuid.uuid4())
                req_id = session_id + "_" + biz_key

                request = MmuChatGptRequest(
                    biz=biz,
                    session_id=session_id,
                    req_id=req_id,
                    config=config,
                    query=prompt
                )

                resp = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(executor, fetch_result, client, request),
                    timeout
                )

                resp = json.loads(MessageToJson(resp))
                if resp['status']['code'] == 'SUCESS' or resp['status']['code'] == "SUCCESS":
                    return resp
                else:
                    print(f"{req_id} 接口调用失败, 状态码={resp['status']['code']}, 重试 {attempts + 1}")
            except asyncio.TimeoutError as e:
                print(f"请求 {req_id} 时发生超时错误: {str(e)}, 重试 {attempts + 1}")
            except Exception as e:
                print(f"请求 {req_id} 时发生错误: {str(e)}, 重试 {attempts + 1}")
        attempts += 1
    return None


async def run_task(client, biz_key, config, entry, prompt_template, limiter):
    prompt = build_prompt(entry, prompt_template)
    result = await run_generator(client, biz_key, config, prompt, limiter)
    if result:
        result = parse_result(entry, result)
        if result:
            return result

    return None


async def run_batch(client, biz_key, config, inputs, prompt_template, limiter, batch_size):
    tasks = []
    olympics_name = inputs["olympics_name"]
    chunks = inputs["chunks"]
    for olympics_knowledge in chunks:
        entry = dict()
        entry["olympics_name"] = olympics_name
        entry["olympics_knowledge"] = olympics_knowledge
        if len(tasks) >= batch_size:
            for completed in asyncio.as_completed(tasks):
                _result = await completed
                yield _result

            tasks = []

        task = asyncio.create_task(
            run_task(client, biz_key, config, entry, prompt_template, limiter)
        )
        tasks.append(task)

    # process not finished tasks
    for completed in asyncio.as_completed(tasks):
        _result = await completed
        yield _result


async def main():
    client_option = ClientOption(
        biz_def='mmu',
        grpc_service_name='mmu-chat-gpt-service',
        grpc_stub_class=MmuChatGptServiceStub
    )
    client = GrpcClient(client_option)

    limiter = aiolimiter.AsyncLimiter(10, 1)

    config = {
        "prompt": "",  # system prompt
        "temperature": "1.0",
        "top_p": "1.0",
        "n": "1",
        "stop": "",
        "presence_penalty": "1.0",
        "frequency_penalty": "0.0",
        "max_tokens": "1024",
        "user": "liuchenxiao"
    }
    biz_key = "gpt-4"

    prompt_template = """
生成10个基于给定奥运会相关的知识有关的问题。要求：
1. 严格根据给出知识去生成问题，不要凭空捏造。
2. 问题不要重复，以最大限度地提高多样性。
3. 问题应该是1到2句话的长度，必须是疑问句。
4  问题中必须出现给定奥运会的名称，比如“北京奥运会”，“伦敦奥运会”等等，不能用“本届奥运会”，“此次奥运会”等这些指代的词。


例如：
参考知识：这是一个和2008年北京奥运会相关的知识片段：
第29届夏季奥林匹克运动会（Beijing 2008；Games of the XXIX Olympiad），又称2008年北京奥运会，2008年8月8日晚上8时整在中国北京开幕。8月24日闭幕。 [16]
主办城市是北京，上海、天津、沈阳、秦皇岛、青岛为协办城市。香港承办马术项目。
此届奥运会共有参赛国家及地区204个，参赛运动员11438人，设28个大项、302小项，共有60000多名运动员、教练员和官员参加。
本届奥运会共创造43项新世界纪录及132项新奥运纪录。北京奥运会闭幕时共有87个国家和地区在赛事中取得奖牌，中国代表团以51枚金牌居金牌榜首名，是奥运历史上首个登上金牌榜首的亚洲国家。
问题：
- 北京奥运会是哪一年举办的？
- 北京奥运会的主办城市是哪个城市，协办城市又有哪些？
- 有多少国家参加了北京奥运会？
- 哪个国家在北京奥运会上取得的金牌数量最多？

请按以下格式返回生成的问题
- 问题1
- 问题2
...

参考知识：这是一个和{olympics_name}相关的知识片段
{olympics_knowledge}
问题：
"""
    output_file = "./olympics_questions.json"
    async with aiofiles.open(output_file, "a") as f:
        async for result in tqdm(
                run_batch(client, biz_key, config, inputs, prompt_template, limiter, batch_size)):
            if result is not None:
                await f.write(json.dumps(result, ensure_ascii=False) + '\n')
                await f.flush()


if __name__ == '__main__':
    asyncio.run(main())
