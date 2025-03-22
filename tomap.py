import pymysql
import folium
import logging

# 配置信息
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'tengxun',
    'charset': 'utf8mb4'
}
#
# 地图默认参数
tiles = 'https://wprd01.is.autonavi.com/appmaptile?x={x}&y={y}&z={z}&lang=zh_cn&size=1&scl=1&style=7'
MAP_CENTER = [32.065, 118.763]  # 南京市中心坐标（备用）
MAP_ZOOM = 12
MAP_TILES = 'CartoDB positron'  # 简洁风格底图
OUTPUT_FILE = 'shop_map.html'

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


def get_valid_coordinates():
    """从数据库获取有效坐标数据"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            sql = '''
                SELECT `名称`, `地址`, `经度`, `纬度`
                FROM t_info
                WHERE `经度` IS NOT NULL
                AND `纬度` IS NOT NULL
                AND `经度` != ''
                AND `纬度` != ''
                AND `经度` != '0'
                AND `纬度` != '0'
            '''
            cursor.execute(sql)
            return cursor.fetchall()

    except Exception as e:
        logging.error(f"数据库查询失败: {str(e)}")
        return []
    finally:
        if conn:
            conn.close()


def generate_map(records, output_file):
    """生成交互式地图"""
    # 初始化地图
    valid_coords = []
    for record in records:
        try:
            lat = float(record[3])
            lng = float(record[2])
            valid_coords.append((lat, lng))
        except ValueError:
            continue

    if valid_coords:
        avg_lat = sum(c[0] for c in valid_coords) / len(valid_coords)
        avg_lng = sum(c[1] for c in valid_coords) / len(valid_coords)
        m = folium.Map(location=[avg_lat, avg_lng],
                       zoom_start=MAP_ZOOM,
                       tiles=tiles,attr='高德-常规图')
    else:
        m = folium.Map(location=MAP_CENTER,
                       zoom_start=MAP_ZOOM,
                       tiles=tiles,attr='高德-常规图')
        logging.warning("使用默认中心坐标")

    # 添加标记点（每个经纬度单独标点）
    for shop in records:
        try:
            name = shop[0]
            address = shop[1]
            lng = float(shop[2])
            lat = float(shop[3])

            # 生成弹出内容
            popup_html = f'''
                <div style="width: 250px;">
                    <h4 style="margin: 2px; color: #2c7bb6;">{name}</h4>
                    <hr style="margin: 5px 0;">
                    <p style="margin: 2px;"><b>地址：</b>{address}</p>
                </div>
            '''

            # 创建标记
            folium.Marker(
                location=[lat, lng],
                popup=folium.Popup(popup_html, max_width=300),
                icon=folium.Icon(
                    color='blue',
                    icon='info-sign',
                    prefix='glyphicon'
                )
            ).add_to(m)

        except Exception as e:
            logging.error(f"处理记录失败：{shop} - {str(e)}")

    # 保存地图文件
    m.save(output_file)
    logging.info(f"地图已生成：{output_file}")


if __name__ == '__main__':
    records = get_valid_coordinates()
    if records:
        generate_map(records, OUTPUT_FILE)
    else:
        logging.warning("没有找到有效的坐标数据")