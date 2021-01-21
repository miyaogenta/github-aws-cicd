import json
import datetime
import urllib.parse
import math
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr

# S3,DynamoDBのインスタンス取得
s3 = boto3.client('s3')
dynamo = boto3.resource('dynamodb')

# 接続テーブル名定義
"""
maxmin_table_name_prod = 'maxmin_count'
realtime_talbe_name_prod = 'realtime_count'
stream_info_table_name_prod = 'stream_info'
maxmin_table_name_dev = 'maxmin_count'
realtime_talbe_name_dev = 'realtime_count'
stream_info_table_name_dev = 'stream_info'
"""

maxmin_table_name = 'maxmin_count'
realtime_table_name = 'realtime_count'
stream_info_table_name = 'stream_info'

# 時間帯別最大・最小人数格納用DB取得メソッド
def get_maxmin_table(stream_id, detected_date, maxmin_table):

    res = maxmin_table.query(
            KeyConditionExpression=Key('stream_id').eq(stream_id)&Key('detected_date').eq(detected_date)
        )
    
    if res['Count'] == 0:
        initialization_list = [0] * 24
        
        #削除日指定(実行日の101日後JST00:00:00のエポック秒)
        delete_time = datetime.datetime.strptime(detected_date, '%Y%m%d')
        delete_time = delete_time + datetime.timedelta(days=101)
        delete_time = delete_time - datetime.timedelta(hours=9)
        delete_time = int(delete_time.timestamp())
        
        item_maxmin = {
            'stream_id': stream_id,
            'detected_date': detected_date,
            'max_people_count_list': initialization_list,
            'min_people_count_list': initialization_list,
            'delete_time': delete_time
        }
        maxmin_table.put_item(Item=item_maxmin)
        
        res = maxmin_table.query(
            KeyConditionExpression=Key('stream_id').eq(stream_id)&Key('detected_date').eq(detected_date)
            )
        
    return res['Items'][0]
    
# リアルタイム人数格納用DB取得メソッド
def get_realtime_table(stream_id, realtime_table):
    res = realtime_table.get_item(
        Key = {'stream_id': stream_id})
    return res['Item']

# ストリーム情報DB取得メソッド
def get_stream_info_table(stream_id, stream_info_table):
    """
    res = stream_info_table.scan(
        FilterExpression=Attr('stream_id').eq(stream_id),
        ConsistentRead=True
        )
    """
    res = stream_info_table.get_item(Key={'stream_id': stream_id})
    return res["Item"]


def lambda_handler(event, context):
    try:
        # 現在時刻取得(JST)
        datetime_now = datetime.datetime.now() + datetime.timedelta(hours=9)
    
        # S3にputされたファイルの中身をjson.laodsしやすいよう変換
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read() # b'テキストの中身'
        bodystr = body.decode('utf-8')
        split_body = bodystr.split('}')
    
        # 接続DB設定
        """
        prod_s3_name = 'peoplecount-stream-data'
        dev_s3_name = 'peoplecount-stream-data-dev'
    
        if bucket == prod_s3_name:
            maxmin_table = dynamo.Table(maxmin_table_name_prod)
            realtime_table = dynamo.Table(realtime_talbe_name_prod)
            stream_info_table = dynamo.Table(stream_info_table_name_prod)
        else:
            maxmin_table = dynamo.Table(maxmin_table_name_dev)
            realtime_table = dynamo.Table(realtime_talbe_name_dev)
            stream_info_table = dynamo.Table(stream_info_table_name_dev)
        """
        
        # 接続DB設定
        maxmin_table = dynamo.Table(maxmin_table_name)
        realtime_table = dynamo.Table(realtime_table_name)
        stream_info_table = dynamo.Table(stream_info_table_name)
        
        # 各DBのデータ取得
        stream_id = key.split('/')[0]
        frame_count = len(split_body)-1
        last_detected_timestamp = json.loads(split_body[frame_count-1]+"}")['detected_timestamp'] 
        detected_date = last_detected_timestamp[0:4] + last_detected_timestamp[5:7] + last_detected_timestamp[8:10]
    
        maxmin_data = get_maxmin_table(stream_id, detected_date, maxmin_table)
        realtime_data = get_realtime_table(stream_id, realtime_table)
        stream_info_data = get_stream_info_table(stream_id, stream_info_table)
        
        # DB登録用データ取得
        
        # 時間帯別最大・最小人数格納用DB登録用項目初期化
        max_people_count_list = [] # 最大人数
        min_people_count_list = [] # 最小人数
        delete_time = '' # 削除日
        
        # 時間帯別最大・最小人数格納用DB登録用項目初期化
        realtime_count = 0 # リアルタイム検知人数
        last_send_mail_datetime = '' # 最終メール日時
        excess_count_current = 0 # 密集度設定人数連続超過回数
    
        # ストリームデータから平均・最大値・最小値を取得    
        people_count_sum = 0
        max_count = 0
        min_count = 99999999
    
        for i in range(frame_count):
            dict_body = json.loads(split_body[i]+"}")
            people_count_sum += dict_body['people_count']
            if (max_count < dict_body['people_count']):
                max_count = dict_body['people_count']
            
            if (min_count > dict_body['people_count']):
                min_count = dict_body['people_count']    
                
        realtime_count = Decimal(str(round(people_count_sum/frame_count, 2))) # 平均計算
        
        # ストリームデータと現在のDB登録値を比較して、最大・最小を決定
        index = int(last_detected_timestamp[11:13])
        max_people_count_list = maxmin_data["max_people_count_list"]
        min_people_count_list = maxmin_data["min_people_count_list"]
        
        # 最大人数比較
        if max_people_count_list[index] < max_count:
            max_people_count_list[index] = max_count
        # 最小人数比較       
        if min_people_count_list[index] == 0 or min_people_count_list[index] > min_count:
            min_people_count_list[index] = min_count
        
        # 削除日取得(DBの値をそのまま再登録)
        delete_time = maxmin_data["delete_time"]
        
        # 最終メール日時、密集度設定人数連続超過回数を取得
        last_send_mail_datetime = realtime_data['last_send_mail_datetime']
        excess_count_current = realtime_data['excess_count_current']
        excess_count_limit = stream_info_data['excess_count_limit']
        dencity_configuration = stream_info_data['dencity_configuration']
        # メール送信の関数を呼び出すかのフラグ
        call_send_mail_func_flg = 0
        
        if stream_info_data["send_mail_flg"] == '1':
            # 最終メール日時をdatetime型に変換して1時間加算
            last_send_mail_datetime_dt = datetime.datetime.strptime(last_send_mail_datetime, "%Y-%m-%d %H:%M:%S")
            last_send_mail_datetime_add_one = last_send_mail_datetime_dt + datetime.timedelta(hours=1)
            
            if math.ceil(realtime_count) < dencity_configuration:
                excess_count_current = 0
            else:
                excess_count_current += 1
                if (excess_count_current >= excess_count_limit) and (datetime_now >= last_send_mail_datetime_add_one):
                    excess_count_current = 0
                    last_send_mail_datetime = datetime_now.strftime('%Y-%m-%d %H:%M:%S')
                    call_send_mail_func_flg = 1
            
        
        # 時間帯別最大・最小人数格納用DB登録処理
        item_maxmin = {
            'stream_id': stream_id,
            'detected_date': detected_date,
            'max_people_count_list': max_people_count_list,
            'min_people_count_list': min_people_count_list,
            'delete_time': delete_time
        }
        maxmin_table.put_item(Item=item_maxmin)
        
        # リアルタイム人数格納用DB登録処理
        item_realtime = {
            'stream_id': stream_id,
            'realtime_count': realtime_count,
            'detected_datetime': last_detected_timestamp,
            'last_send_mail_datetime': last_send_mail_datetime,
            'excess_count_current': excess_count_current
        }
        realtime_table.put_item(Item=item_realtime)
        
        # メールプッシュ用Lambda呼び出し
        if call_send_mail_func_flg == 1:
            user_id = stream_info_data['company_id']
            camera_name = stream_info_data['installation_place']
            
            event = {
                'user_id':user_id,
                'stream': {
                    'stream_id': stream_id,
                    'camera_name': camera_name,
                    'threshold': int(dencity_configuration)
                }
            }
            
            response = boto3.client('lambda').invoke(
                FunctionName='send-alert-email',
                InvocationType='RequestResponse',
                Payload=json.dumps(event)
            )
            
        return 0
    except Exception as e:
        print(e)
        raise e
