# -*- coding: utf-8 -*-
import json
# import xmltodict
# import xml.etree.ElementTree as ET

class Config:
    """
    json 형태의 config 파일을 로딩한다.
    connection info
    """
    def load_jconfig(self, conf_file):
        with open( conf_file )  as f:
            self.jconfig = json.load(f)

    """
    xml 형태의  config 파일을 로딩한다.
    query 파일 
    """
    # def load_xconfig ( self, conf_file):
    #     tree = ET.parse( conf_file )
    #     self.root = tree.getroot()

    """
    xml element 중 지정된 element 이하정보만 get한다. ( 상위노드는 제외 )
    """
    # def get_xquerydict(self,Nodename):
    #     return self.root.findall(Nodename)

    """
    json config 정보에서 지정한 요소만 get 한다.
    """
    def get_jsection(self, section):
        return self.jconfig[ section ]
