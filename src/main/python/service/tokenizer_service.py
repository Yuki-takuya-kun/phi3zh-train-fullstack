import argparse

import asyncio
from transformers import AutoTokenizer
from tokenize_pb2 import TextList, TokenList, Tokens

parser = argparse.ArgumentParser(description="Start a Tokenizer Service")
parser.add_argument("--model_dir", type=str, help="Path of tokenizer model")
parser.add_argument("--port", type=int, default=7983, help="Port to run the tokenizer")
args = parser.parse_args()

tokenizer = AutoTokenizer.from_pretrained(args.model_dir)

async def handle_client(reader:asyncio.StreamReader, writer:asyncio.StreamWriter):
    try:
        while True:
            raw_length = await reader.read(4)
            if not raw_length:
                break
            data_length = int.from_bytes(raw_length, byteorder='big')
            data = bytearray()
            while len(data) < data_length:
                packet = await reader.read(data_length - len(data))
                if not packet:
                    break
                data.extend(packet)
            if len(data) < data_length: break
            textList = TextList()
            textList.ParseFromString(bytes(data))
            texts = [text for text in textList.text]
            tokenLists = tokenizer.batch_encode_plus(texts)['input_ids']
            result = [Tokens(tokens=tokenlist) for tokenlist in tokenLists]
            ProtoTokenList = TokenList(list=result)
            result = ProtoTokenList.SerializeToString()
            output_length = len(result).to_bytes(4, byteorder='big')
            writer.write(output_length + result)
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

async def run_server():
    server = await asyncio.start_server(handle_client, '0.0.0.0', args.port)
    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    asyncio.run(run_server())

