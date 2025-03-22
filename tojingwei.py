import pymysql
import requests
import logging
from urllib.parse import quote
import time

# 配置信息
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'tengxun',
    'charset': 'utf8mb4'
}

AMAP_KEY = '068e1996176dd322a32f13b477f1efe9'
BATCH_SIZE = 100  # 每批处理数量
RETRY_TIMES = 3  # 失败重试次数
SLEEP_INTERVAL = 100  # 新增：每处理100条休息
SLEEP_SECONDS = 2  # 新增：休息时长

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('geocode.log'),
        logging.StreamHandler()
    ]
)


def get_connection():
    """获取数据库连接"""
    return pymysql.connect(**DB_CONFIG)


def geocode_address(address):
    """调用高德地理编码API"""
    url = f'https://restapi.amap.com/v3/geocode/geo?address={quote(address)}&output=json&key={AMAP_KEY}'

    for _ in range(RETRY_TIMES):
        try:
            response = requests.get(url, timeout=10)
            data = response.json()

            if data['status'] == '1' and int(data['count']) > 0:
                location = data['geocodes'][0]['location']
                return tuple(location.split(','))

            logging.warning(f"地址解析失败: {address} - {data.get('info', '')}")
            return (None, None)

        except Exception as e:
            logging.error(f"API请求异常: {str(e)}")
            time.sleep(2)

    return (None, None)


def update_coordinates():
    """批量更新坐标数据"""
    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            # 查询需要处理的记录
            sql = '''
                SELECT `地址`, 名称 
                FROM t_info 
                WHERE (`经度` IS NULL OR `纬度` IS NULL) 
                AND `地址` IS NOT NULL 
                AND `地址` != ''
            '''
            cursor.execute(sql)
            results = cursor.fetchall()

            processed = 0
            request_count = 0  # 新增：请求计数器

            while results:
                for address, name in results:
                    # 调用地理编码API
                    lng, lat = geocode_address(address)

                    # 更新数据库
                    update_sql = '''
                        UPDATE t_info 
                        SET `经度` = %s, `纬度` = %s 
                        WHERE `地址` = %s AND 名称 = %s
                    '''
                    cursor.execute(update_sql, (lng, lat, address, name))

                    logging.info(f"已更新: {name} | {address} => ({lng}, {lat})")
                    processed += 1
                    request_count += 1  # 计数器累加

                    # 新增：每处理100条休息
                    if request_count % SLEEP_INTERVAL == 0:
                        logging.info(f"已达到 {SLEEP_INTERVAL} 次请求，休息 {SLEEP_SECONDS} 秒...")
                        time.sleep(SLEEP_SECONDS)

                conn.commit()
                results = cursor.fetchmany(BATCH_SIZE)

                # 原有批次间隔保留（可选）
                time.sleep(0.1)

            logging.info(f"处理完成！共更新 {processed} 条记录")

    except Exception as e:
        conn.rollback()
        logging.error(f"数据库操作异常: {str(e)}")
    finally:
        conn.close()


if __name__ == '__main__':
    update_coordinates()