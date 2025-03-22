import pymysql
import numpy as np
import logging
from tqdm import tqdm
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import folium
from folium.plugins import HeatMap
from matplotlib.font_manager import FontProperties  # 导入FontProperties
#
plt.rcParams['font.family'] = 'SimHei'

# 配置信息
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'tengxun',
    'charset': 'utf8mb4'
}

# 区域边界坐标
POINT_A = (118.538936, 32.105357)  # 经度, 纬度
POINT_B = (119.125925, 31.849542)
N = 9  # 分区数量 2^9=512

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


class GeoAnalyzer:
    def __init__(self):
        self.min_lon = min(POINT_A[0], POINT_B[0])
        self.max_lon = max(POINT_A[0], POINT_B[0])
        self.min_lat = min(POINT_A[1], POINT_B[1])
        self.max_lat = max(POINT_A[1], POINT_B[1])

        # 计算分割数 (16x32=512)
        self.lon_splits = 2 ** (N // 2)  # 16
        self.lat_splits = 2 ** ((N + 1) // 2)  # 32

        # 计算步长
        self.lon_step = (self.max_lon - self.min_lon) / self.lon_splits
        self.lat_step = (self.max_lat - self.min_lat) / self.lat_splits

        # 初始化统计容器
        self.grid_stats = np.zeros(
            (self.lon_splits, self.lat_splits, 2),
            dtype=np.float64
        )  # [count, sum_reviews]

    def parse_reviews(self, review_str):
        """将'12条点评'转换为数值"""
        if not review_str:
            return 0
        try:
            return int(review_str.replace('条点评', ''))
        except:
            return 0

    def get_grid_index(self, lon, lat):
        """计算坐标所属网格索引"""
        if not (self.min_lon <= lon <= self.max_lon and
                self.min_lat <= lat <= self.max_lat):
            return None

        i = int((lon - self.min_lon) // self.lon_step)
        j = int((lat - self.min_lat) // self.lat_step)

        # 处理边界情况
        i = min(i, self.lon_splits - 1)
        j = min(j, self.lat_splits - 1)

        return (i, j)

    def load_data(self):
        """从数据库加载数据"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cursor:
                sql = '''
                    SELECT `经度`, `纬度`, `点评数量` 
                    FROM t_info 
                    WHERE `经度` IS NOT NULL 
                    AND `纬度` IS NOT NULL
                '''
                cursor.execute(sql)
                return cursor.fetchall()

        except Exception as e:
            logging.error(f"数据库查询失败: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def analyze(self):
        """执行分析流程"""
        # 加载数据
        records = self.load_data()
        if not records:
            logging.warning("没有找到有效数据")
            return

        # 处理每条记录
        for lon_str, lat_str, reviews in tqdm(records, desc="处理数据"):
            try:
                lon = float(lon_str)
                lat = float(lat_str)
                review_num = self.parse_reviews(reviews)

                grid_idx = self.get_grid_index(lon, lat)
                if grid_idx is None:
                    continue

                i, j = grid_idx
                self.grid_stats[i, j, 0] += 1  # 计数
                self.grid_stats[i, j, 1] += review_num  # 评论数累加

            except Exception as e:
                logging.error(f"数据处理异常: {str(e)}")

    def save_results(self):
        """保存统计结果到数据库"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            with conn.cursor() as cursor:
                # 创建结果表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS grid_stats (
                        grid_id INT PRIMARY KEY,
                        lon_start FLOAT,
                        lon_end FLOAT,
                        lat_start FLOAT,
                        lat_end FLOAT,
                        shop_count INT,
                        avg_reviews FLOAT
                    )
                ''')

                # 插入数据
                for i in range(self.lon_splits):
                    for j in range(self.lat_splits):
                        count = self.grid_stats[i, j, 0]
                        total = self.grid_stats[i, j, 1]
                        avg = total / count if count > 0 else 0

                        # 计算网格边界
                        lon_start = self.min_lon + i * self.lon_step
                        lon_end = lon_start + self.lon_step
                        lat_start = self.min_lat + j * self.lat_step
                        lat_end = lat_start + self.lat_step

                        # 生成唯一网格ID
                        grid_id = i * self.lat_splits + j

                        cursor.execute('''
                            INSERT INTO grid_stats 
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            shop_count=VALUES(shop_count),
                            avg_reviews=VALUES(avg_reviews)
                        ''', (grid_id, lon_start, lon_end,
                              lat_start, lat_end, count, avg))

                conn.commit()
                logging.info(f"成功保存 {self.lon_splits * self.lat_splits} 个网格数据")

        except Exception as e:
            conn.rollback()
            logging.error(f"数据库保存失败: {str(e)}")
        finally:
            if conn:
                conn.close()

# 区域参数
LON_SPLITS = 16  # 经度分割数（根据实际分区设置）
LAT_SPLITS = 32  # 纬度分割数

def load_grid_stats():
    """从数据库加载网格统计数据"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute('''
                SELECT lon_start, lat_start, shop_count, avg_reviews 
                FROM grid_stats 
                ORDER BY grid_id
            ''')
            return cursor.fetchall()
    except Exception as e:
        print(f"数据库查询失败: {str(e)}")
        return []
    finally:
        if conn:
            conn.close()

def show_grid():
    # 初始化统计矩阵
    count_matrix = np.zeros((LON_SPLITS, LAT_SPLITS))
    avg_matrix = np.zeros((LON_SPLITS, LAT_SPLITS))

    # 填充数据
    stats = load_grid_stats()
    if stats:
        for idx, (lon_start, lat_start, count, avg) in enumerate(stats):
            i = idx // LAT_SPLITS  # 经度索引
            j = idx % LAT_SPLITS  # 纬度索引
            count_matrix[i, j] = count
            avg_matrix[i, j] = avg

    # 绘制热力图
    plt.figure(figsize=(15, 6))

    # 店铺数量热力图
    plt.subplot(1, 2, 1)
    plt.imshow(count_matrix.T, origin='lower', cmap='YlOrRd',
               extent=[118.538936, 119.125925, 31.849542, 32.105357])
    plt.colorbar(label='店铺数量')
    plt.title('店铺分布热力图')
    plt.xlabel('经度')
    plt.ylabel('纬度')

    # 平均评论数热力图
    plt.subplot(1, 2, 2)
    plt.imshow(avg_matrix.T, origin='lower', cmap='Blues',
               extent=[118.538936, 119.125925, 31.849542, 32.105357])
    plt.colorbar(label='平均评论数')
    plt.title('评论热度图')
    plt.xlabel('经度')

    plt.tight_layout()
    plt.show()


def generate_interactive_map():
    """生成交互式地图"""
    tiles = 'https://wprd01.is.autonavi.com/appmaptile?x={x}&y={y}&z={z}&lang=zh_cn&size=1&scl=1&style=7'

    # 初始化地图中心点
    m = folium.Map(
        location=[(32.105357 + 31.849542) / 2, (118.538936 + 119.125925) / 2],
        zoom_start=10,
        tiles=tiles,
        attr='高德-常规图'
    )

    # 加载数据
    stats = load_grid_stats()
    if not stats:
        print("无有效数据")
        return m

    # 添加热力图层
    heat_data = []
    for lon_start, lat_start, count, avg in stats:
        if count > 0:
            # 计算网格中心点
            lon_center = lon_start + (119.125925 - 118.538936) / (2 * LON_SPLITS)
            lat_center = lat_start + (32.105357 - 31.849542) / (2 * LAT_SPLITS)
            heat_data.append([lat_center, lon_center, count])

    HeatMap(heat_data, radius=15, blur=20).add_to(m)

    # 添加网格标记
    for lon_start, lat_start, count, avg in stats:
        if count > 0:
            # 计算网格边界
            lon_end = lon_start + (119.125925 - 118.538936) / LON_SPLITS
            lat_end = lat_start + (32.105357 - 31.849542) / LAT_SPLITS

            # 生成弹出信息
            popup_html = f'''
                <b>网格统计</b><hr>
                店铺数: {count}<br>
                均评数: {avg:.1f}<br>
                经度范围: {lon_start:.6f}~{lon_end:.6f}<br>
                纬度范围: {lat_start:.6f}~{lat_end:.6f}
            '''

            # 添加矩形框
            folium.Rectangle(
                bounds=[[lat_start, lon_start], [lat_end, lon_end]],
                color='#ff7800',
                fill=True,
                fill_color='#ffff00',
                fill_opacity=0.2,
                popup=folium.Popup(popup_html, max_width=300)
            ).add_to(m)

    return m
def jiaohu():
    # 生成并保存地图
    map_obj = generate_interactive_map()
    map_obj.save('grid_map.html')
    print("地图已保存至 grid_map.html")
if __name__ == '__main__':
    # analyzer = GeoAnalyzer()
    # analyzer.analyze()
    # analyzer.save_results()
    #show_grid()
    jiaohu()