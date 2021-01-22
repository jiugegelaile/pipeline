# -*- coding: utf-8 -*- 
# @Time : 2020/12/2 10:14 
# @Author : Li Xiaopeng
# @File : version_util.py
# @Software: PyCharm


import json
import os
import re
import sys

import requests

from navinfo import get_logger


class StorageEngine:
    # 开发环境
    # __ops_login_url = "http://10.70.191.234:30002/api/v1/auth/do_login"
    # __storage_engine_url = "http://10.70.151.208/dev/datahub/api/v1"
    #外网开发环境
    # __ops_login_url = "http://10.70.191.234:30002/api/v1/auth/do_login"
    # __storage_engine_url = "http://10.60.145.142/dev/datahub/api/v1"

    #外网生产环境
    # __ops_login_url = "http://10.60.145.142:80/api/v1/auth/do_login"
    # __storage_engine_url = "http://10.60.145.142:80/datahub/api/v1"

    # 生产环境
    __ops_login_url = "http://10.70.191.234:31002/api/v1/auth/do_login"
    __storage_engine_url = "http://10.70.151.208/datahub/api/v1"

    __logger = get_logger("DEBUG", "navinfo.StorageEngine")

    # 租户ID，用户名， 密码
    __tenant_id, __username, __password = None, None, None

    # 输入数据集，输出数据集
    __source_dataset_map, __sink_dataset_map = None, None

    # 输出日志级别，默认为 error
    __log_level = "error"

    def __init__(self, username, password, tenant_id="", log_level="ERROR", auth_token=None, authorization=None):
        r"""
        从环境变量中读取配置信息
        """
        self.__username = username
        self.__password = password
        self.__tenant_id = tenant_id

        self.__log_level = log_level.lower()

        if os.getenv("TOKEN_LOGIN_URL") is not None:
            self.__ops_login_url = os.getenv("TOKEN_LOGIN_URL")
        if os.getenv("STORAGEENGINE_URL") is not None:
            self.__storage_engine_url = os.getenv("STORAGEENGINE_URL")

        if os.getenv("NAVCLOUD_VERSION_SOURCE_INFO") is not None:
            self.__source_dataset_map = json.loads(os.getenv("NAVCLOUD_VERSION_SOURCE_INFO"))
        if os.getenv("NAVCLOUD_VERSION_SINK_INFO") is not None:
            self.__sink_dataset_map = json.loads(os.getenv("NAVCLOUD_VERSION_SINK_INFO"))

        # env_f = "/etc/navinfo_env.conf"
        # if os.path.exists(env_f):
        #     if open(env_f).readline().strip() == "dev":
        #         self.__ops_login_url = "http://10.70.191.234:30002/api/v1/auth/do_login"
        #         self.__storage_engine_url = "http://10.70.151.208/dev/datahub/api/v1"
        # else:
        #     self.__ops_login_url = "http://10.70.191.234:31002/api/v1/auth/do_login"
        #     self.__storage_engine_url = "http://10.70.151.208/datahub/api/v1"
        if all([auth_token, authorization]):
            self.__auth_token, self.__authorization = auth_token, authorization
        else:
            self.__init_tokens()

    def __init_tokens(self):
        r"""
        从认证服务器获取访问存储引擎需要的Token信息
        """
        auth_data = """{
            "username": "%s",
            "password": "%s"
        }""" % (self.__username, self.__password)
        try:
            res_token = requests.post(url=self.__ops_login_url, data=auth_data)
            res_str = res_token.content.decode("utf-8")
        except Exception as e:
            self.__logger.error("登录服务器连接异常，无法获取auth-token!!")
            raise e
        json_obj = None
        try:
            json_obj = json.loads(res_str)
            self.user_info = json_obj
            self.__authorization = json_obj["result"]["data"]["id"]
            self.__auth_token = json_obj["result"]["data"]["AuthToken"]
            self.__logger.info("获取的authorization：%s, auth_token: %s" % (self.__authorization, self.__auth_token))
        except Exception as e:
            self.__logger.error("登录服务器返回数据异常，无法获取auth-token\n%s" % json_obj)
            raise Exception(res_str)

    def __get_source_dsid_path(self, tag):
        if tag in self.__source_dataset_map:
            source_info = self.__source_dataset_map.get(tag)
            dataset_id = source_info["datasetId"]
            dir_path: str = source_info["path"]
            if dir_path.rindex("/") > 0:
                dir_path = dir_path[len(dataset_id) + 1:]
            else:
                dir_path = "/"
            dir_path = re.sub("/+", "/", dir_path)
            # self.__logger.info("source 输出的 dir_path为“%s”" % dir_path)
            return dataset_id, dir_path
        else:
            self.__logger.error("请输入正确的输入 TAG，您输入的 “%s” 不存在!" % tag)

    def __get_sink_dsid_path(self):
        dataset_id = self.__sink_dataset_map["datasetId"]
        dir_path: str = self.__sink_dataset_map["path"]
        if dir_path.rindex("/") > 0:
            dir_path = dir_path[len(dataset_id) + 1:]
        else:
            dir_path = "/"
        dir_path = re.sub("/+", "/", dir_path)
        # self.__logger.info("sink 输出的 dir_path为“%s”" % dir_path)
        return dataset_id, dir_path

    def __create_dir(self, dataset_id, dir_path):
        url = f"{self.__storage_engine_url}/datasets/{dataset_id}/path?tenantId={self.__tenant_id}"
        payload = "{\"path\": \"" + dir_path + "\"}"
        headers = {
            "Content-Type": "application/json",
            "auth-token": self.__auth_token
        }
        res = requests.request("POST", url, headers=headers, data=payload)
        if res.status_code != 200:
            self.__logger.error(f"文件夹\"{dir_path}\"创建失败！！")
            raise Exception(f"文件\"{dir_path}\"创建失败！！")

    # 从存储引擎查看目录下文件列表
    def __list_files(self, dataset_id, dir_path):
        r"""
        :param dataset_id: 输入数据集中ID
        :param dir_path: 输入数据集中目录的绝对路径，需要查看其中的文件列表
        """
        files = []
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.__authorization,
            "auth-token": self.__auth_token
        }
        page_num = 1
        page_size = 1000
        total_num = 999999999999
        while len(files) < total_num:
            list_dir_url = self.__storage_engine_url + "/datasets/{}/path?tenantId=%s&path=" % self.__tenant_id
            path = list_dir_url.format(dataset_id) + dir_path + "&pageNo=%s&size=%s" % (page_num, page_size)
            try:
                res = requests.get(path, headers=headers)
            except Exception as e:
                self.__logger.error(e)
                raise e
            res_json = json.loads(res.content.decode("utf-8"))
            if res_json["code"] == "0":
                self.__logger.info("-----本次请求url————————————：%s " % path)
                self.__logger.info("文件列表数量：“%s”" % res_json["result"]["data"])
                files += res_json["result"]["data"]
                total_num = int(res_json["result"]["pageBean"]["totalElements"])
                if len(files) == total_num:
                    break
            else:
                self.__logger.error(res_json)
                sys.exit(-1)
            page_num += 1
        return files

    @staticmethod
    def __match_add_file(file_arr, file, regex_str, by_abs_path):
        if regex_str and len(regex_str) > 0:
            if by_abs_path and re.match(regex_str, file["logiclPath"]):
                file_arr.append(file)
            else:
                if re.match(regex_str, file["name"]):
                    file_arr.append(file)
        else:
            file_arr.append(file)

    def __filter_files(self, file_arr, dataset_id, dir_path, regex_str, recursion=False, by_abs_path=False):
        files = self.__list_files(dataset_id, dir_path)
        for file in files:
            self.__match_add_file(file_arr, file, regex_str, by_abs_path)
            if recursion and file["folder"]:
                logical_path = file["logiclPath"]
                if file.get("logiclPath") is not None and file.get("path") is not None \
                        and file.get("logiclPath") == file.get("path"):
                    logical_path = logical_path[len(dataset_id) + 1:]
                self.__filter_files(file_arr, dataset_id, logical_path, regex_str, recursion, by_abs_path)

    @staticmethod
    def __process_list_res(dataset_id, res_arr, dir_path, keep_real=False, util_api=True):
        if keep_real:
            return res_arr
        else:
            file_arr = list()
            for tmp_f in res_arr:
                logical_path: str = tmp_f["logiclPath"][len(dataset_id) + 1:]
                if dir_path != "/" and util_api:
                    logical_path = logical_path[len(dir_path):]
                file_arr.append({
                    "name": tmp_f["name"],
                    "path": logical_path,
                    "size": tmp_f["size"],
                    "isFile": not tmp_f["folder"]
                })
            return file_arr

    @staticmethod
    def __process_rsql_res(res_arr, dir_path, keep_real=False):
        if keep_real:
            return res_arr
        else:
            file_arr = list()
            for tmp_f in res_arr:
                file_arr.append({
                    "name": tmp_f["name"],
                    "path": tmp_f["logical_path"][len(dir_path):],
                    "size": tmp_f["size"],
                    "isFile": tmp_f["is_file"] == 1
                })
            return file_arr

    def __remote_file(self, dataset_id, remote_file):
        download_url = "%s/datasets/%s/data/downloadS3?path=%s&tenantId=%s" % \
                       (self.__storage_engine_url, dataset_id, remote_file, self.__tenant_id)
        headers = {
            'Authorization': self.__authorization,
            'auth-token': self.__auth_token
        }
        try:
            res = requests.request("GET", download_url, headers=headers)
        except Exception as e:
            self.__logger.error(f"文件\"{remote_file}\"下载失败！！")
            raise e
        return res

    def __download_file(self, dataset_id, remote_file, local_dir_path: str, file_name, force_create=True):
        if self.__log_level in ["debug", "info"]:
            self.__logger.info("download %s's '%s' to %s/%s" % (dataset_id, remote_file, local_dir_path, file_name))
        res = self.__remote_file(dataset_id, remote_file)
        if res.status_code != 200:
            self.__logger.error(f"文件\"{remote_file}\"下载失败！！")
            raise Exception(f"文件\"{remote_file}\"下载失败！！")
        else:
            if not os.path.exists(local_dir_path):
                if force_create:
                    os.makedirs(local_dir_path)
                else:
                    raise Exception(f"下载时发现本地文件夹不存在：{local_dir_path}")
            if local_dir_path.endswith(("/", "\\")):
                local_file = local_dir_path + file_name
            else:
                local_file = local_dir_path + "/" + file_name
            with open(local_file, "wb") as file:
                file.write(res.content)
                file.flush()
                file.close()

    def __upload_file(self, local_file_path, dataset_id, remote_dir_path):
        if self.__log_level in ["info", "warn", "error"]:
            self.__logger.info("upload local '%s' to %s's ‘%s’" % (local_file_path, dataset_id, remote_dir_path))
        payload = {
            'tenantId': self.__tenant_id,
            'path': remote_dir_path
        }
        files = [
            ('file', open(local_file_path, 'rb'))
        ]
        headers = {
            "Authorization": self.__authorization,
            "auth-token": self.__auth_token
        }
        upload_url = self.__storage_engine_url + "/datasets/%s/data/uploadS3" % dataset_id
        try:
            response = requests.request("POST", upload_url, headers=headers, data=payload, files=files)
        except Exception as e:
            self.__logger.error(e)
            raise e
        res_json = json.loads(response.content.decode('utf8'))
        if res_json["code"] == "0":
            return res_json
        else:
            err_info = "上传数据失败， %s！" % res_json
            self.__logger.error(err_info)
            sys.exit(-1)

    def source_info(self):
        return self.__source_dataset_map

    def sin_info(self):
        return self.__sink_dataset_map

    def set_source_info(self, source_info_map):
        self.__source_dataset_map = source_info_map

    def set_sink_info(self, sink_info_map):
        self.__sink_dataset_map = sink_info_map

    def set_tenant_id(self, tenant_id):
        self.__tenant_id = tenant_id

    def list_datasets(self):
        url = f"{self.__storage_engine_url}/datasets?tenantId={self.__tenant_id}&size=10000"
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.__authorization,
            "auth-token": self.__auth_token
        }
        response = requests.request("GET", url, headers=headers)
        res_str = response.content.decode('utf8')
        print(res_str)
        dataset_l = json.loads(res_str)["result"]["data"]
        return dataset_l

    def create_dataset(self, name, dataset_type="B10", storage_type=1, description="", source="", volume=1, access="0"):
        r"""创建数据集
        :param name: 本地文件的路径
        :param dataset_type: B10：照片
        :param storage_type: 1 版本数据；2 流式数据；3 异变缓存数据；4 索引数据
        :return 上传后文件在数据集中的描述信息
        """
        create_url = f"{self.__storage_engine_url}/datasets?tenantId={self.__tenant_id}"
        payload = {
            "datasetType": dataset_type,
            "description": description,
            "name": name,
            "source": source,
            "storageType": storage_type,
            "volume": volume,
            "access": f"{access}"
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.__authorization,
            "auth-token": self.__auth_token
        }
        response = requests.request("POST", create_url, headers=headers, data=json.dumps(payload))
        return json.loads(response.content.decode('utf8'))

    def create_dir(self, tag, create_path):
        dataset_id, dir_path = self.__get_source_dsid_path(tag)
        dir_p = re.sub("/+", "/", dir_path + "/" + create_path)
        self.__create_dir(dataset_id, dir_p)

    def create_dataset_dir(self, dataset_id, abs_path):
        self.__create_dir(dataset_id, abs_path)



    def list_fold(self, tag):
        """
        获取文件夹名称，用于判断执行的模型操作
        Args:
            dataset_id:
            dir_path:

        Returns:

        """
        dataset_id, dir_path = self.__get_source_dsid_path(tag)
        files = self.__list_files(dataset_id, dir_path)
        folders = []
        for file in files:
            if file["folder"]:

                logical_path = file["logiclPath"]
                if file.get("logiclPath") is not None and file.get("path") is not None \
                        and file.get("logiclPath") == file.get("path"):
                    logical_path = logical_path[len(dataset_id) + 2:]
                    folders.append(logical_path)
            else:
                folders = None
        return folders


    def list(self, tag, regex_str="", recursion=False, by_abs_path=False, keep_real=False):
        r"""根据传入的正则表达式来过滤目录
        :param tag: 输入数据目录的标识
        :param regex_str: 正则表达式，匹配它的文件才会放入返回文件列表
        :param recursion: 是否递归显示所有子目录
        :param by_abs_path: 匹配正则表达式时使用绝对路径，False表示使用文件名称进行匹配，True表示使用绝对路径匹配。默认False
        """
        res_arr = list()
        dataset_id, dir_path = self.__get_source_dsid_path(tag)
        self.__logger.info("------------开始过滤需要的文件filter_files-------------")
        self.__filter_files(res_arr, dataset_id, dir_path, regex_str, recursion, by_abs_path)
        self.__logger.info("------------结束过滤filter_files-------------")
        file_arr = self.__process_list_res(dataset_id, res_arr, dir_path, keep_real)
        return file_arr

    def list_dataset(self, dataset_id, remote_dir: str, regex_str="", recursion=False, by_abs_path=False,
                     keep_real=False):
        r"""根据传入的正则表达式来过滤目录
        :param dataset_id: 查看的目标数据集 ID
        :param remote_dir: 查看的目标数据集中的目录
        :param regex_str: 正则表达式，匹配它的文件才会放入返回文件列表
        :param recursion: 是否递归显示所有子目录
        :param by_abs_path: 匹配正则表达式时使用绝对路径，False表示使用文件名称进行匹配，True表示使用绝对路径匹配。默认False
        """
        dataset_id = str(dataset_id)
        remote_dir = re.sub("/+", "/", remote_dir)
        if len(remote_dir) > 1 and remote_dir.endswith("/"):
            remote_dir = remote_dir[0:len(remote_dir) - 1]
        res_arr = []
        self.__filter_files(res_arr, dataset_id, remote_dir, regex_str, recursion, by_abs_path)
        file_arr = self.__process_list_res(dataset_id, res_arr, remote_dir, keep_real, util_api=False)
        return file_arr

    def list_by_rsql(self, tag, query, keep_real=False):
        r"""根据传入的正则表达式来过滤目录
        :param tag: 输入数据目录的标识
        :param query: RSQL的匹配条件，匹配它的文件才会放入返回文件列表
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.__authorization,
            "auth-token": self.__auth_token
        }
        dataset_id, dir_path = self.__get_source_dsid_path(tag)
        dir_path = re.sub("/+", "/", dir_path + "/")
        query = "logical_path==%s*;%s" % (dir_path, query)
        search_url = self.__storage_engine_url + "/datasets/%s/data/search?query=%s&tenantId=%s" % \
                     (dataset_id, query, self.__tenant_id)
        try:
            res = requests.get(search_url, headers=headers)
        except Exception as e:
            self.__logger.error(e)
            sys.exit(-1)
        res_json = json.loads(res.content.decode("utf-8"))
        if res_json["code"] == "0":
            res_arr = res_json["result"]["data"]
        else:
            self.__logger.error(res_json)
            sys.exit(-1)
        file_arr = self.__process_rsql_res(res_arr, dir_path, keep_real)
        return file_arr

    def list_dataset_by_rsql(self, dataset_id, query, keep_real=False):
        r"""根据传入的正则表达式来过滤目录
        :param dataset_id: 查询的目标数据集 ID
        :param query: RSQL的匹配条件，匹配它的文件才会放入返回文件列表
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.__authorization,
            "auth-token": self.__auth_token
        }
        file_arr = []
        search_url = self.__storage_engine_url + "/datasets/%s/data/search?query=%s&tenantId=%s" % \
                     (dataset_id, query, self.__tenant_id)
        try:
            res = requests.get(search_url, headers=headers)
        except Exception as e:
            self.__logger.error(e)
            sys.exit(-1)
        res_json = json.loads(res.content.decode("utf-8"))
        res_arr = list()
        if res_json["code"] == "0":
            res_arr = res_json["result"]["data"]
        else:
            self.__logger.error(res_json)
        file_arr += self.__process_rsql_res(res_arr, "", keep_real)
        return file_arr

    def upload(self, local_file: str, remote_dir: str):
        r"""上传单个文件到输出数据集中的目录下
        :param local_file: 本地文件的路径
        :param remote_dir: 输出数据集中的目录，本地文件上传后放入该目录
        :return 上传后文件在数据集中的描述信息
        """
        dataset_id, dir_path = self.__get_sink_dsid_path()
        real_remote_path = dir_path + "/" + remote_dir + "/"
        real_remote_path = re.sub("/+", "/", real_remote_path)
        real_remote_path = real_remote_path[0:len(real_remote_path) - 1]
        if not os.path.exists(local_file):
            self.__logger.error("本地文件系统中：%s 文件不存在!!!" % local_file)
            sys.exit(-1)
        return self.__upload_file(local_file, self.__sink_dataset_map["datasetId"], real_remote_path)

    def upload_file_to_dataset(self, local_file: str, dataset_id, remote_dir: str):
        r"""上传单个文件到输出数据集中的目录下
        :param local_file: 本地文件的路径
        :param dataset_id: 上传的目标数据集 ID
        :param remote_dir: 输出数据集中的目录，本地文件上传后放入该目录
        :return 上传后文件在数据集中的描述信息
        """
        real_remote_path = remote_dir + "/"
        real_remote_path = re.sub("/+", "/", real_remote_path)
        real_remote_path = real_remote_path[0:len(real_remote_path) - 1]
        if not os.path.exists(local_file):
            self.__logger.error("本地文件系统中：%s 文件不存在!!!" % local_file)
            sys.exit(-1)
        return self.__upload_file(local_file, dataset_id, real_remote_path)

    def download(self, tag: str, remote_file: str, local_dir: str):
        r"""
        :param tag: 输入tag
        :param remote_file: 远程文件，在输入数据集中的绝对路径
        :param local_dir: 本地目录，存放下载的数据文件
        """
        dataset_id, dir_path = self.__get_source_dsid_path(tag)
        real_remote_path = dir_path + "/" + remote_file + ""
        real_remote_path = re.sub("/+", "/", real_remote_path)
        file_name = real_remote_path[real_remote_path.rindex("/") + 1:]
        return self.__download_file(dataset_id, real_remote_path, local_dir, file_name)

    def download_dataset_file(self, dataset_id, remote_file: str, local_dir: str):
        r"""
        :param dataset_id: 待下载文件坐在的数据集ID
        :param remote_file: 远程文件，在输入数据集中的绝对路径
        :param local_dir: 本地目录，存放下载的数据文件
        """
        real_remote_path = re.sub("/+", "/", remote_file)
        file_name = real_remote_path[real_remote_path.rindex("/") + 1:]
        return self.__download_file(dataset_id, real_remote_path, local_dir, file_name)

    def open_remote_file(self, tag, remote_file):
        r"""上传单个文件到输出数据集中的目录下
        :param tag: 远程文件所在的数据数据集标志 TAG
        :param remote_file: 待打开文件在数据集中的路径
        :return 上传后文件在数据集中的描述信息
        """
        res = self.__remote_file(self.__source_dataset_map[tag]["datasetId"], remote_file)
        return res.content

    def open_remote_dataset_file(self, dataset_id, remote_file):
        r"""上传单个文件到输出数据集中的目录下
        :param dataset_id: 待打开文件所在数据集 ID
        :param remote_file: 待打开文件在数据集中的路径
        :return 上传后文件在数据集中的描述信息
        """
        res = self.__remote_file(dataset_id, remote_file)
        return res.content
