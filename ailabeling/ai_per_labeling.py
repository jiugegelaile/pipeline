# -*- coding: utf-8 -*- 
# @Time : 2020/12/2 10:15 
# @Author : Li Xiaopeng
# @File : ai_per_labeling.py
# @Software: PyCharm

import base64
import json
import os
import sys
import time
import uuid

import redis
import requests

from navinfo import StorageEngine
from navinfo import get_logger
from navinfo.utils.constants import sparkSession

THREAD_NUM = 4
BATCH_SIZE = 20

#外网测试redis
# REDIS_IP = "10.60.2.79"
# REDIS_PORT = 18888
# REDIS_PWD = "password"
# REDIS_DB_INDEX = 1

#内网redis
REDIS_IP = "10.70.151.254"
REDIS_PORT = 6379
REDIS_PWD = "Shpd@dp.com"
REDIS_DB_INDEX = 1


# REDIS_UPDATED_DOC_IDS = "already_ids"

LOCAL_DIR = "/tmp/ailabeling/%s" % os.getpid()
os.makedirs(LOCAL_DIR)

ES_CONN = {"connected": True}


class BatchInfo:
    batch_id = 0
    batch_size = 0
    job_key = ""
    images_key = ""
    progress_key_d = {}
    res_key_d = {}

    def __init__(self, batch_id, job_key):
        self.batch_id = batch_id
        self.job_key = job_key
        self.images_key = ""
        self.progress_key_d = {}
        self.res_key_d = {}


class AiPreLabeling:

    def compute_iou(self, rec1, rec2):
        '''
        Compute IOU
        :param rec1:rectangle1.
        :param rec2:rectangle2.
        :return:The IOU of two rectangles.
        '''
        cx1, cy1, cx2, cy2 = rec1
        gx1, gy1, gx2, gy2 = rec2

        S_rec1 = (cx2 - cx1) * (cy2 - cy1)
        S_rec2 = (gx2 - gx1) * (gy2 - gy1)

        x1 = max(cx1, gx1)
        y1 = max(cy1, gy1)
        x2 = min(cx2, gx2)
        y2 = min(cy2, gy2)

        w = max(0, x2 - x1)
        h = max(0, y2 - y1)
        area = w * h

        iou = area / (S_rec1 + S_rec2 - area)
        return iou

    def combine_ai_obj(self, img_json_arr, name):
        local_objs = []
        local_json_path = r"D:\toBiaoZhu\training\labels"
        if os.path.exists(local_json_path):
            json_p = f"{local_json_path}/{name}.json"
            local_json = open(json_p, "r", encoding="UTF-8").readline()
            local_objs = json.loads(local_json)["objects"]
        combine_objs = [] + local_objs
        print("-----------------img_json_arr:", img_json_arr)

        for img_json in img_json_arr:
            if img_json.get("objects") is not None:
                objects = img_json["objects"]
                print('--------------object type:',type(objects))
                if objects is not None and len(objects) > 0:
                    filter_element_index = []
                    for inx,obj in enumerate(objects):
                        #根据f_code过滤结果
                        print('==================================f_code:',obj['f_code'])
                        if  obj['f_code'] == 'TS':
                            obj['f_code'] = 'NI01'
                        if  obj['f_code'] == '0E':
                            obj['f_code'] ='NI03'

                        if  str(obj['f_code']) not in CUSTOM_CODE:
                            filter_element_index.append(inx)
                        print("==============================filter_element_index:", filter_element_index)

                        if obj['f_code'] in property_map.keys():
                            properties = {}
                            for property in property_map[obj['f_code']]:
                                properties[property['value']] = property['default']
                            obj['properties'] = properties

                        # 只有识别到point的对象才有效
                        obj_valid = True
                        obj_points = obj["obj_points"]
                        if len(obj_points) != 0:
                            if len(obj_points[0]) == 3 or len(obj_points[0]) == 2:  # 多边形
                                obj["obj_type"] = 2
                            elif len(obj_points[0]) == 5:  # 矩形
                                obj["obj_type"] = 1

                            # 合并标注结果时，将AI预处理的结果与人工标注对比，重合度大于0.5则抛弃
                            if COMBINE_LABELING:
                                if len(local_objs) > 0 and obj["obj_type"] == 1:
                                    for pt in obj_points:
                                        del pt["f_conf"]
                                        obj_rec = (pt["x"], pt["y"], pt["x"] + pt["w"], pt["y"] + pt["h"])
                                        for l_obj in local_objs:
                                            l_pt = l_obj["obj_points"][0]
                                            l_obj_rec = (l_pt["x"], l_pt["y"], l_pt["x"] + l_pt["w"], l_pt["y"] + l_pt["h"])
                                            iou = self.compute_iou(obj_rec, l_obj_rec)
                                            if iou > 0.5:
                                                obj_valid = False
                                                break
                            else:
                                obj["obj_source"] = 1  # 标注来源，1、AI算法，2：人工；3、人工+AI
                                obj["obj_id"] = str(uuid.uuid4())
                        if obj_valid:
                            combine_objs.append(obj)
                    for i in sorted(filter_element_index, reverse=True):
                        del combine_objs[i]
                    print("-------过滤后json结果：%s" % combine_objs)
        return json.dumps(combine_objs)

    def process_hash_result(self, redis_conn, se, batch_info: BatchInfo):

        hash_arr = []

        for key in batch_info.res_key_d.values():
            hash_arr.append(redis_conn.hgetall(key))

        for key in hash_arr[0]:
            img_path = key.decode("utf8")
            img_json_arr = []
            for hash_t in hash_arr:
                if hash_t[key].decode() == '' or hash_t[key].decode() is None:
                    json_obj = {"cv_task": 1, "obj_num": 0, "objects": []}
                else:
                    json_obj = json.loads(hash_t[key].decode("utf8"))
                img_json_arr.append(json_obj)
            name_start_pos = img_path.rindex("/") + 1
            file_name = img_path[name_start_pos:]
            name = file_name[:file_name.rindex(".")]
            res_json = self.combine_ai_obj(img_json_arr, name)
            content = bytes(res_json, encoding="utf8")
            file_abs_path = "%s/%s.json" % (LOCAL_DIR, name)

            with open(file_abs_path, "wb") as file:
                file.write(content)
                file.flush()
            se.upload(file_abs_path, "img_result")
            os.remove(file_abs_path)
            # redis_conn.sadd(REDIS_UPDATED_DOC_IDS, img_path)

    def process_ai_result(self, redis_conn, se, batch_info: BatchInfo):

        while True:
            ai_complete_num = 0
            for progress_key in batch_info.progress_key_d.values():
                if redis_conn.exists(progress_key):
                    progress = int(redis_conn.get(progress_key).decode("utf8"))
                    if progress == batch_info.batch_size:
                        ai_complete_num += 1
            # logger.info("process_ai_result:  %s   %s" % (ai_complete_num, len(AI_JOB_INFO)))

            if ai_complete_num == len(AI_JOB_INFO):
                logger.info("第 %s 批次开始处理AI结果...." % batch_info.batch_id)
                self.process_hash_result(redis_conn, se, batch_info)
                # for res_key in batch_info.res_key_d.values():
                #     redis_conn.delete(res_key)
                # for progress_key in batch_info.progress_key_d.values():
                #     redis_conn.delete(progress_key)
                # redis_conn.delete(batch_info.images_key)
                break

            # 暂停3秒再继续执行
            time.sleep(3)

    def start_ai_job(self, redis_conn, batch_info: BatchInfo, url, ai_token, tag):
        logger.info("start_ai_job")

        redis_conn.delete(batch_info.res_key_d[tag])
        redis_conn.delete(batch_info.progress_key_d[tag])

        redis_address = "%s:%s" % (REDIS_IP, REDIS_PORT)
        payload = """
                {
                    "inference_type":1,
                    "src_provider": {"type": 0, "address": "%s","password": "%s","database": %s,
                        "batch_id": "%s"},
                    "dst_provider": {"type": 0,"address": "%s","password": "%s","database": %s,
                        "result_key": "%s","progress_key": "%s"}
                }""" % \
                  (redis_address, REDIS_PWD, REDIS_DB_INDEX, batch_info.images_key, redis_address, REDIS_PWD, REDIS_DB_INDEX,
                   batch_info.res_key_d[tag], batch_info.progress_key_d[tag])

        headers = {
            'Content-Type': 'application/json',
            'token':ai_token
        }
        logger.info("--------------------headers:%s" % headers)
        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            json_content = json.loads(response.content.decode('utf8'))
            logger.info("start_ai_job:  %s %s %s" % (tag, json_content, response))
        except Exception as e:
            logger.info("AI 接口访问异常%s" % e)

    def start_ai_jobs(self, redis_conn, batch_info: BatchInfo):
        for key in AI_JOB_INFO:
            ai_times = int(redis_conn.incr(AI_JOB_INFO[key]["AI_TIMES_KEY"], 1))
            # ai_url = AI_JOB_INFO[key]["AI_URLS"][ai_times % len(AI_JOB_INFO[key]["AI_URLS"])]
            # ai_token = AI_JOB_INFO[key]["TOKEN"][ai_times % len(AI_JOB_INFO[key]["TOKEN"])]
            ai_url = AI_JOB_INFO[key]["AI_URLS"][0]
            ai_token = AI_JOB_INFO[key]["TOKEN"][0]
            self.start_ai_job(redis_conn, batch_info, ai_url, ai_token, key)

    def process_batch(self, se, redis_conn, batch_info: BatchInfo, file_dict: dict):
        img_num = 0
        pull_img_start_time = time.time()
        for image_path in file_dict:
            img_info = file_dict[image_path]
            # is_updated = redis_conn.sismember(REDIS_UPDATED_DOC_IDS, image_path)
            # if is_updated:
            #     continue
            logger.info("batch_id: %s, pull img: %s" % (batch_info.batch_id, image_path))
            local_path = "%s/%s" % (LOCAL_DIR, img_info["name"])
            se.download("A", image_path, LOCAL_DIR)
            file_con = open(local_path, mode="rb").read()
            print("==============================image_path:",image_path)
            redis_conn.hset(batch_info.images_key, image_path, file_con)
            os.remove(local_path)
            img_num += 1

        pull_img_cost_time = time.time() - pull_img_start_time
        logger.info("第 %s 批拉取数据耗时： %s" % (batch_info.batch_id, pull_img_cost_time))

        if img_num > 0:
            batch_info.batch_size = img_num
            self.start_ai_jobs(redis_conn, batch_info)
            self.process_ai_result(redis_conn, se, batch_info)

    def init_batch_info(self, batch_info: BatchInfo):
        images_key = "%s_%s_imgs" % (batch_info.job_key, batch_info.batch_id)
        batch_info.images_key = images_key
        for key in AI_JOB_INFO:
            result_key = "%s_%s_%s_result" % (batch_info.job_key, batch_info.batch_id, key)
            batch_info.res_key_d[key] = result_key
            progress_key = "%s_%s_%s_progress" % (batch_info.job_key, batch_info.batch_id, key)
            batch_info.progress_key_d[key] = progress_key

    def process_images(self, jpg_iter):
        print('-------------------开始图像处理程序：%s' % jpg_iter)

        file_num = 0
        jpg_files = []
        if jpg_iter is not None:
            for j_f in jpg_iter:
                jpg_files.append(j_f)
                file_num += 1

        redis_conn = redis.Redis(host=REDIS_IP, port=REDIS_PORT, password=REDIS_PWD, db=REDIS_DB_INDEX)

        job_key = "task_" + str(uuid.uuid4())
        logger.info(job_key)
        redis_conn.delete(job_key)

        logger.info("本 TASK 处理 %s 张照片。" % file_num)

        ai_process = AiPreLabeling()

        batch_num = file_num / BATCH_SIZE + 1
        for batch_id in range(int(batch_num)):
            start_idx = batch_id * BATCH_SIZE
            end_idx = start_idx + BATCH_SIZE
            if end_idx > file_num:
                end_idx = file_num
            file_dict = {}
            for idx in range(start_idx, end_idx):
                f_info = jpg_files[idx]
                file_dict[f_info["path"]] = f_info
            batch_info = BatchInfo(batch_id, job_key)
            ai_process.init_batch_info(batch_info)
            ai_process.process_batch(se, redis_conn, batch_info, file_dict)
            logger.info("第 %s 批处理完成。" % batch_info.batch_id)

        redis_conn.delete(job_key)
        return ["%s 张照片已完成" % file_num]


if __name__ == "__main__":
    logger = get_logger("INFO", name="PySparkAILabeling")
    try:
        args = sys.argv
        pl_arg_dict = json.loads(base64.b64decode(args[len(args) - 1]).decode("utf8"))
        source_info = pl_arg_dict["pl_source"]
        sink_info = pl_arg_dict["pl_sink"]
        env_args = pl_arg_dict["pl_env"]
        AI_JOB_INFO = env_args["AI_JOB_INFO"]
        dataset_id = env_args["DATASET_ID"]
        auth_token_labeling = env_args["AUTHTOKEN"]
        task_id = env_args["TASKID"]
        kind = env_args['KIND']
        custom_code_property = env_args['CODE']
        label_type = env_args['TYPE']
        logger.info("-------------env_args: % s" % env_args)
        logger.info("-------------task_id: %s" % task_id)
        se = StorageEngine(env_args["USERNAME"], env_args["PASSWORD"], env_args["TENANT_ID"], log_level='INFO')
        # se.set_source_info(source_info)
        # se.set_sink_info(sink_info)
        se.set_source_info({"A": {"name": "notebook", "datasetId": str(dataset_id), "path": "/" + str(dataset_id), "nodeType": 1}})
        se.set_sink_info({"name": "aa", "datasetId": str(dataset_id), "path": "/" + str(dataset_id), "nodeType": 1})
        logger.info("---------source_info:%s" % source_info)
        logger.info("----------sink_info:%s" % sink_info)


        # kind = 1
        # label_type = 1
        # custom_code_property = [{"classCode":"0a0004","propertyList":[{"default":"是","value":"清晰度1"},{"default":"否","value":"遮挡率1"},{"default":"是","value":"遮挡率11"}]},{"classCode":"071007","propertyList":[{"default":"否","value":"清晰度3"},{"default":"是","value":"遮挡率3"}]}]
        CUSTOM_CODE = [i['classCode'] for i in custom_code_property]
        property_map = {}
        for i in custom_code_property:
            if "propertyList" in i.keys():
                property_map[i["classCode"]] = i["propertyList"]
        logger.info('-------------------custom code: %s' % CUSTOM_CODE)
        logger.info('-------------------custom property :%s' % property_map)
        # datased_id = "3219"
        # auth_token = 'eyJuYW1lIjoiY2xvdWRvcHNzZXJ2aWNlIiwidGltZXN0YW1wIjoiMTYwNzU4Njk2NzAzOSIsInV1aWQiOiJiMWFkZGZmOWNhODM0NDhiYjU0ODMxNTJmODBlZDhlYSIsInVzZXJJZCI6ImNiNDBlZjNmZDFhNjRlYTc4ZmM0NzgyYWQ0YWExYTNmMzg2MmIxNTlhZGFkNDVjZDgxMzI1MWVhNDQ1M2Y3YjMiLCJ1c2VybmFtZSI6ImRhdGFvcHMiLCJkaXNwbGF5bmFtZSI6ImRhdGFvcHM2MDc1NyIsInNpZ24iOiJaV0ppT0dVNU5EZGxOR1ZqTmpjM1l6VmxOV1l6WVdVeVpESXhORFppTmpVeVpqY3lNalk1TkE9PSJ9'
        # auth = 'gAAAAABf0dSWHqcfsk1PLR6sV-0UslTxbRT6iNmL2t7eIUeF3FjKZlaeAKNrjGgRbcAuA3iqJ498xJdRVSvT0HFtstq99CMvS7P4lOoQ7axr2KBJNiAbGa4YNucDy9UwGKvqyCzz1wCeVYkGB3ATVVw9j4ZJwf1OFnpfYimgQRvfrLq1HbBlHgA2ucVUDnHa-bSy5sMIZcrySyb-JPdRrcOLhdRYGrEI6Now3VUIiRwrm-fc8DcHljY'
        # se = StorageEngine("dataops", "123.com", "baad5c29ff844da08be9b0e618790044", log_level='INFO', auth_token=auth_token, authorization=auth)
        # se.set_source_info({"A": {"name": "notebook", "datasetId": "3219", "path": "/3219", "nodeType": 1}})
        # se.set_sink_info({"name": "aa", "datasetId": "3219", "path": "/3219", "nodeType": 1})

        folders = se.list_fold("A")
        logger.info("-----------------------folders: %s" % folders)



        # AI_JOB_INFO = {
        #     "BOARD": {
        #         "AI_TIMES_KEY": "board_ai_times",
        #         "AI_URLS": ["http://10.60.2.79:16001/image/drivingdetectionfortrafficsign/v4"],
        #         "TOKEN": ["ahfsjdkhfsjdkhfjksdhfjksdhfudksj"]
        #     }
        # }



        # 获取待处理的图片列表

        COMBINE_LABELING = False
        logger.info("PySparkAILabeling--开始！！")

        # if len(jpg_files) != 0:
        #     AiPreLabeling().process_images(jpg_files)
        spark = sparkSession()
        ai_pre_labeling = AiPreLabeling()

        jpg_files = []
        if "datasource_pt" in folders and len(folders) > 1:
            """点云处理"""
            # jpg_files = se.list("A", recursion=True, regex_str=".*.jpg")
            # logger.info("-------------jpg_files:%s" % jpg_files)
            logger.info("-------------点云适配处理-----------------")
        if kind == 1 or label_type == 1 or kind == 2:
            """图像预标注"""
            logger.info("------------图像预标注----------")
            jpg_files = se.list("A", recursion=True, regex_str=".*.jpg")
            logger.info("-------------jpg_files:%s" % jpg_files)

            jpg_rdd = spark.sparkContext.parallelize(jpg_files, numSlices=3)
            # logger.info("-----------jpg_rdd: %s " % jpg_rdd.collect())
            res_rdd = jpg_rdd.mapPartitions(ai_pre_labeling.process_images)
            res_rdd.foreach(print)
            #os.rmdir(LOCAL_DIR)
        # if kind == 2:
        #     """连续帧处理"""
        #     AI_JOB_INFO = {
        #         "BOARD": {
        #             "AI_TIMES_KEY": "board_ai_times",
        #             "AI_URLS": ["http://192.167.8.220:28001/image/drivingdetectionfortrafficsign/v4"],
        #             "TOKEN": ["ahfsjdkhfsjdkhfjksdhfjksdhfudksj"]
        #         }
        #     }
        #
        #     logger.info("---------连续帧预处理----------")
        #     #连续帧筛选
        #     jpg_files = se.list("A", recursion=True, regex_str=".*.jpg")
        #     logger.info("-------------jpg_files:%s" % jpg_files)
        #     temp_dict = {}
        #     for jpg_file in jpg_files:
        #         if jpg_file['name'].split('_')[0] not in temp_dict.keys():
        #             temp_dict[jpg_file['name'].split('_')[0]] = [jpg_file]
        #         else:
        #             temp_dict[jpg_file['name'].split('_')[0]].append(jpg_file)
        #     temp_jpg_files = [temp_dict[i] for i in temp_dict.keys()]
        #     logger.info('++++++++++++++++++++temp_jpg_files: %s' % temp_jpg_files)
        #
        #
        #     #jpg_files = [('1',test_a),('2',test_b)]
        #     # spark = sparkSession()
        #     # ai_pre_labeling = AiPreLabeling()
        #     for batch in temp_jpg_files:
        #         logger.info('-----------------开始：%s' % batch)
        #         jpg_rdd = spark.sparkContext.parallelize(batch, numSlices=1)
        #         logger.info("-----------jpg_rdd: %s " % jpg_rdd.collect())
        #         res_rdd = jpg_rdd.mapPartitions(lambda x: ai_pre_labeling.process_images(x))
        #         #res_rdd = jpg_rdd.map(lambda x: ai_pre_labeling.process_images([x]))
        #         #res_rdd = jpg_rdd.reduceByKey(lambda x: AiPreLabeling().process_images(x))
        #         #logger.info("-----------res_rdd: %s " % res_rdd.collect())    #加上之后会运行两次相同的数据
        #         res_rdd.foreach(print)
        #         #os.rmdir(LOCAL_DIR)
        #
        # logger.info("PySparkAILabeling--完成！！")
        # os.rmdir(LOCAL_DIR)
        #dev:18893   uat：18401
        # logger.info("处理完成，回调Labeling---")
        # url = f"http://10.73.1.103:18401/labelservice/task/finishPre?taskId={task_id}"
        # headers = {
        #     "auth-token": auth_token_labeling
        # }
        # try:
        #     res = requests.get(url, headers=headers)
        #     logger.info("-------回调返回结果：%s" % res)
        # except Exception as e:
        #     logger.error(e)

    except Exception as e:
        logger.error(e)


