import json
import typing
from fastapi import Depends, FastAPI, HTTPException

value =  [ {"key1" : "value", "key2" : "value2"} , {"key1" : "value", "key2" : "value2"} ]


# for i in value:
#     print( json.dumps(i))


import rudder_analytics

rudder_analytics.write_key = '1wYxNFWqZHmRxdn2tOKSOQLBVON'
rudder_analytics.data_plane_url = "http://localhost:8000"

rudder_analytics.track('developer_user_id', 'Simple Track Event', {
  'key1': 'val1'
})
# testvalue= {
#     "batch": [{
#             "userId": "identified user id",
#             "anonymousId": "anon-id-new",
#             "type": "identify",
#             "context": {
#                 "traits": {
#                     "trait1": "new-val"
#                 },
#                 "ip": "14.5.67.21",
#                 "library": {
#                     "name": "http"
#                 }
#             },
#             "timestamp": "2020-02-02T00:23:09.544Z"
#         },
#         {
#             "userId": "identified user id",
#             "anonymousId": "anon-id-new",
#             "event": "Product Purchased new",
#             "type": "track",
#             "properties": {
#                 "name": "Shirt",
#                 "revenue": 4.99
#             },
#             "context": {
#                 "ip": "14.5.67.21",
#                 "library": {
#                     "name": "http"
#                 }
#             },
#             "timestamp": "2020-02-02T00:23:09.544Z"
#         },
#  ],
# "sentAt": "2021-08-23T07:39:13.384516+00:00"
# }
#
# def batch_send2kafka(msgs: dict):
#     batchs = msgs['batch']
#     sentAt = msgs['sentAt']
#     for msg in batchs:
#         msg['sentAt'] = sentAt
#         print( json.dumps(msg))
#
# batch_send2kafka( testvalue )
# #print( json.dumps(testvalue))
# def test():
#   try:
#       raise Exception( "test exception")
#   except Exception as ex:
#       print( ex.args )
#       raise HTTPException(status_code=500, detail= ex.args ) from None
#
#
# try:
#   test()
# except  HTTPException as ex:
#   print ( ex.detail )