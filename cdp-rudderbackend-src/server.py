import uvicorn
import secrets
import json
import typing

from fastapi import Depends, FastAPI, HTTPException, status, Request, Response, Body
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from confluent_kafka import KafkaException
from kafkasend import AIOProducer


tags_metadata = [
    {
        "name": "sourceconfig",
        "description": "client api에서 config 정보를 가져온다.",
    },
    {
        "name": "identify",
        "description": "Manage items. So _fancy_ they have their own docs.",
        "externalDocs": {
            "description": "Items external docs",
            "url": "https://fastapi.tiangolo.com/",
        },
    },
]

app = FastAPI(openapi_tags=tags_metadata)
app.router.redirect_slashes = False

origins = [
    "http://localhost:8000",
    "http://0.0.0.0:8000",
]

app.add_middleware(
    CORSMiddleware,
    # allow_origins=origins,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBasic()

app.mount("/html", StaticFiles(directory="html"), name="html")
# app.mount("/main", StaticFiles(directory="main"), name="main")


# for control plane server reposne data : it will be fulled by config information
configJson = {}
writeKey = "1wYxNFWqZHmRxdn2tOKSOQLBVON"
configkafka = {"bootstrap.servers": "192.168.2.84:9092"}
MAX_SINGLE_MSG_LEN = 32 * 1024 #32K
MAX_BATCH_MSG_LEN  = 4 * 1024 * 1024 #4M

aio_producer = None

@app.on_event("startup")
async def startup_event():
    global configJson, aio_producer
    configjson_str = open('./datacontrol/sourceConfig', 'r').read()
    # configjson_str = open('./main/cloudConfig', 'r').read()
    configJson = json.loads(configjson_str)
    aio_producer = AIOProducer(configkafka)
    print(f"start up read content={configJson}")

@app.on_event("shutdown")
def shutdown_event():
    aio_producer.close()

# request body 크기가 제한된 크기를 초과하는 지 검사
def check_if_oversize(maxsize: int, s: str) -> bool:
    msglen = len( s.encode('utf-8'))
    print("msg len : {}".format(msglen) )
    if msglen > maxsize :
        return True
    else:
        return False

# json object 에 해당 key가 있는지 검사
def is_json_key_present(data, key):
    try:
        buf = data[key]
    except KeyError:
        return False
    return True

# HTTP Basic Auth 인증
def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, writeKey)
    correct_password = secrets.compare_digest(credentials.password, "")
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect id or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


async def send2kafka(msg: str):
    try:
        result = await aio_producer.produce("testtopic", bytes(msg, encoding="utf-8"))
        return {"timestamp": result.timestamp(), "ret" : "ok"}
    except KafkaException as ex:
        raise HTTPException(status_code=500, detail=ex.args[0].str())

async def batch_send2kafka(allmsgs: dict):
    batchs = allmsgs['batch']
    if is_json_key_present( allmsgs, 'sentAt'):
        sentAt = allmsgs['sentAt']
    for msg in batchs:
        if is_json_key_present(allmsgs, 'sentAt'):
            msg['sentAt'] = sentAt
        print('each batch msg : ' + json.dumps(msg))
        result = await aio_producer.produce("testtopic", bytes(json.dumps(msg,ensure_ascii=False), encoding="utf-8"))

    return {"timestamp": result.timestamp(), "ret" : "ok"}

# rudder api 에서 sourceConfig/?p=web&v=1.1.18 로 호출한다.
#  /sourceConfig 로 할 경우 fastAPI에서 307 temporary redirect 시킨다. 부득이 /sourceConfig/로 수정
@app.get("/sourceConfig/", tags=["sourceconfig"], status_code=status.HTTP_200_OK)
async def soureconfig(request: Request, apiid: str = Depends(get_current_username) ):
    """
    client sdk API 에서 config 정보를 가져오기 위한 URI 이다.
    """
    global configJson
    print("apiid = " + apiid)
    print(f"source get read content={configJson}")
    headers = {"access-control-allow-credentials": "true",
               "access-control-allow-origin": "*",
               "strict-transport-security": "max-age=15552000; includeSubDomains"
               }
    return JSONResponse(content=configJson, headers=headers)

@app.post("/v1/identify", status_code = 200 , tags=["identify"])
async def identify( response: Response,
                    apiid: str = Depends(get_current_username),
                    request: Request = Body (
                        ...,
                        example={
                            "name": "Foo",
                            "description": "A very nice Item",
                            "price": 35.4,
                            "tax": 3.2,
                        },
                    ),
                    ):
    try:
        req_text = await request.json()
        print("identify body = " + "\n" + json.dumps(req_text, indent=2, sort_keys=True))
        msg_str = json.dumps(req_text, ensure_ascii=False)
        if check_if_oversize ( MAX_SINGLE_MSG_LEN, msg_str ) :
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"code": "C0111", "message": "Too long Request "}
        await send2kafka(msg_str)
        return {"response": "ok"}
    except Exception as ex:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"code": "C0111", "message": ex.args}

@app.post("/v1/page", status_code = 200 )
async def page(request: Request, response: Response, apiid: str = Depends(get_current_username)):
    try:
        req_text = await request.json()
        print("page body = " + "\n" + json.dumps(req_text, indent=2, sort_keys=True))
        msg_str = json.dumps(req_text, ensure_ascii=False)
        if check_if_oversize ( MAX_SINGLE_MSG_LEN, msg_str ) :
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"code": "C0111", "message": "Too long Request "}
        await send2kafka(msg_str)
        return {"response": "ok"}
    except Exception as ex:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"code": "C0111", "message": ex.args}

@app.post("/v1/track", status_code = 200 )
async def page(request: Request, response: Response, apiid: str = Depends(get_current_username)):
    try:
        req_text = await request.json()
        print("track body = " + "\n" + json.dumps(req_text, indent=2, sort_keys=True))
        msg_str = json.dumps(req_text, ensure_ascii=False)
        if check_if_oversize ( MAX_SINGLE_MSG_LEN, msg_str ) :
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"code": "C0111", "message": "Too long Request "}
        await send2kafka(msg_str)
        return {"response": "ok"}
    except Exception as ex:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"code": "C0111", "message": ex.args}

@app.post("/v1/screen", status_code = 200 )
async def screen(request: Request, response: Response, apiid: str = Depends(get_current_username)):
    try:
        req_text = await request.json()
        print("screen body = " + "\n" + json.dumps(req_text, indent=2, sort_keys=True))
        msg_str = json.dumps(req_text, ensure_ascii=False)
        if check_if_oversize ( MAX_SINGLE_MSG_LEN, msg_str ) :
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"code": "C0111", "message": "Too long Request "}
        await send2kafka(msg_str)
        return {"response": "ok"}
    except Exception as ex:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"code": "C0111", "message": ex.args}

@app.post("/v1/batch", status_code = 200 )
async def batch(request: Request, response: Response, apiid: str = Depends(get_current_username)):
    try:
        req_text = await request.json()
        print("batch  body = " + "\n" + json.dumps(req_text, indent=2, sort_keys=True))
        msg_str = json.dumps(req_text, ensure_ascii=False)
        if check_if_oversize(MAX_BATCH_MSG_LEN, msg_str):
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"code": "C0111", "message": "Too long Request "}
        await batch_send2kafka(req_text)
        return {"response": "ok"}
    except Exception as ex:
        response.status_code =  status.HTTP_400_BAD_REQUEST
        return { "code": "C0111", "message": ex.args }

@app.post("/v1/alias" , status_code = 200 )
async def alias(request: Request, response: Response, apiid: str = Depends(get_current_username)):
    try:
        req_text = await request.json()
        print("alias  body = " + "\n" + json.dumps(req_text, indent=2, sort_keys=True))
        msg_str = json.dumps(req_text, ensure_ascii=False)
        if check_if_oversize ( MAX_SINGLE_MSG_LEN, msg_str ) :
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"code": "C0111", "message": "Too long Request "}
        await send2kafka(msg_str)
        return {"response": "ok"}
    except Exception as ex:
        response.status_code =  status.HTTP_400_BAD_REQUEST
        return { "code": "C0111", "message": ex.args }

@app.post("/v1/group", status_code = 200 )
async def group(request: Request, response: Response, apiid: str = Depends(get_current_username)):
    try:
        req_text = await request.json()
        print("alias group = " + "\n" + json.dumps(req_text, indent=2, sort_keys=True))
        msg_str = json.dumps(req_text, ensure_ascii=False)
        if check_if_oversize ( MAX_SINGLE_MSG_LEN, msg_str ) :
            response.status_code = status.HTTP_400_BAD_REQUEST
            return {"code": "C0111", "message": "Too long Request "}
        await send2kafka(msg_str)
        return {"response": "ok"}
    except Exception as ex:
        response.status_code =  status.HTTP_400_BAD_REQUEST
        return { "code": "C0111", "message": ex.args }

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)