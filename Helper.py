import mysql.connector
from mysql.connector import Error
from datetime import datetime


class MySqlHelper:
    def __init__(self, host, user, password, database, port=3306):
        """初始化数据库连接参数"""
        self.config = {
            'localhost': host,
            'root': user,
            '12345678': password,
            'test': database,
            'port': port
        }
        print(f"Config: {self.config}")  # 调试：打印配置
        self.connection = None
        self.cursor = None

    def connect(self):
        """建立数据库连接"""
        try:
            self.connection = mysql.connector.connect(
                host=self.config['localhost'],
                user=self.config['root'],
                password=self.config['12345678'],
                database=self.config['test'],
                port=self.config['port']
            )
            self.cursor = self.connection.cursor(prepared=True)
            print("Successfully connected to the database")
        except Error as e:
            print(f"Connection failed: {e}")
            raise

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Database connection closed")

    def insert_student(self, student_id, name, height=None):
        """插入一名学生记录"""
        query = "INSERT INTO students (student_id, name, height) VALUES (%s, %s, %s)"
        try:
            self.connect()
            self.cursor.execute(query, (student_id, name, height))
            self.connection.commit()
            print(f"Inserted student {student_id}")
            return True
        except Error as e:
            print(f"Error inserting student: {e}")
            return False
        finally:
            self.close()

    def update_student(self, student_id, name=None, height=None):
        """更新学生信息，支持部分更新"""
        query_parts = []
        params = []
        if name is not None:
            query_parts.append("name = %s")
            params.append(name)
        if height is not None:
            query_parts.append("height = %s")
            params.append(height)

        if not query_parts:
            print("No fields to update")
            return False

        query = f"UPDATE students SET {', '.join(query_parts)} WHERE student_id = %s"
        params.append(student_id)

        try:
            self.connect()
            self.cursor.execute(query, params)
            self.connection.commit()
            updated = self.cursor.rowcount > 0
            print(f"Updated {self.cursor.rowcount} student(s)")
            return updated
        except Error as e:
            print(f"Error updating student: {e}")
            return False
        finally:
            self.close()

    def delete_student(self, student_id):
        """删除指定学号的学生"""
        query = "DELETE FROM students WHERE student_id = %s"
        try:
            self.connect()
            self.cursor.execute(query, (student_id,))
            self.connection.commit()
            deleted = self.cursor.rowcount > 0
            print(f"Deleted {self.cursor.rowcount} student(s)")
            return deleted
        except Error as e:
            print(f"Error deleting student: {e}")
            return False
        finally:
            self.close()

    def get_student(self, student_id):
        """根据学号查询学生信息"""
        query = "SELECT student_id, name, height FROM students WHERE student_id = %s"
        try:
            self.connect()
            self.cursor.execute(query, (student_id,))
            result = self.cursor.fetchone()
            if result:
                return {'student_id': result[0], 'name': result[1], 'height': result[2]}
            print(f"No student found with ID {student_id}")
            return None
        except Error as e:
            print(f"Error fetching student: {e}")
            return None
        finally:
            self.close()

    def get_all_students(self):
        """查询所有学生信息"""
        query = "SELECT student_id, name, height FROM students"
        try:
            self.connect()
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return [{'student_id': row[0], 'name': row[1], 'height': row[2]} for row in results]
        except Error as e:
            print(f"Error fetching students: {e}")
            return []
        finally:
            self.close()

    def get_max_id(self, table_name):
        """获取指定表的最大 id"""
        query = f"SELECT MAX(id) FROM {table_name}"
        try:
            self.connect()
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            max_id = result[0] if result[0] is not None else 0
            return max_id
        except Error as e:
            print(f"Error fetching max id from {table_name}: {e}")
            return 0
        finally:
            self.close()

    def insert_hot_searches(self, hot_list):
        """
        批量插入百度热搜到 hot_searches 表，动态分配 id
        hot_list: 列表，每个元素为 {'rank': int, 'keyword': str, 'heat': int}
        返回：成功插入的记录数
        """
        if not hot_list:
            print("No data to insert")
            return 0

        query = """
        INSERT INTO hot_searches (id, keyword, heat, crawl_time) 
        VALUES (%s, %s, %s, %s)
        """
        params = []
        max_id = self.get_max_id('hot_searches')
        for idx, item in enumerate(hot_list):
            try:
                keyword = item['keyword']
                heat = item['heat']
                params.append((max_id + idx + 1, keyword, heat, datetime.now()))
            except KeyError as e:
                print(f"Invalid data format in hot_list: missing {e}")
                continue

        if not params:
            print("No valid data to insert")
            return 0

        try:
            self.connect()
            inserted_count = 0
            for param in params:
                print(f"Inserting: {param}")
                self.cursor.execute(query, param)
                inserted_count += self.cursor.rowcount
            self.connection.commit()
            print(f"Successfully inserted {inserted_count} hot search records")
            return inserted_count
        except Error as e:
            print(f"Error inserting hot searches: {e}")
            if self.connection:
                self.connection.rollback()
            return 0
        finally:
            self.close()


    def insert_movie(self, movie_rank, title, director=None, year=None, rating=None, reviews_count=None, genre=None, country=None, description=None):
        """插入一条电影记录，动态分配 id"""
        max_id = self.get_max_id('movies')
        query = """
        INSERT IGNORE INTO movies (id, movie_rank, title, director, year, rating, reviews_count, genre, country, description) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            self.connect()
            self.cursor.execute(query, (max_id + 1, movie_rank, title, director, year, rating, reviews_count, genre, country, description))
            self.connection.commit()
            print(f"Inserted movie: {title} (Rank: {movie_rank}, ID: {max_id + 1})")
            return True
        except Error as e:
            print(f"Error inserting movie: {e}")
            return False
        finally:
            self.close()

    def insert_movies(self, movie_list):
        """
        批量插入豆瓣电影到 movies 表，动态分配 id
        movie_list: 列表，每个元素为 {'movie_rank': int, 'title': str, 'director': str, 'year': int, 'rating': float, 'reviews_count': int, 'genre': str, 'country': str, 'description': str}
        返回：成功插入的记录数
        """
        if not movie_list:
            print("No movies to insert")
            return 0

        query = """
        INSERT IGNORE INTO movies (id, movie_rank, title, director, year, rating, reviews_count, genre, country, description) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = []
        max_id = self.get_max_id('movies')
        for idx, movie in enumerate(movie_list):
            try:
                params.append((
                    max_id + idx + 1,
                    movie['movie_rank'],
                    movie['title'],
                    movie.get('director'),
                    movie.get('year'),
                    movie.get('rating'),
                    movie.get('reviews_count'),
                    movie.get('genre'),
                    movie.get('country'),
                    movie.get('description')
                ))
            except KeyError as e:
                print(f"Invalid data format in movie_list: missing {e}")
                continue

        if not params:
            print("No valid movies to insert")
            return 0

        try:
            self.connect()
            inserted_count = 0
            for param in params:
                print(f"Inserting movie: {param[2]} (Rank: {param[1]}, ID: {param[0]})")
                self.cursor.execute(query, param)
                inserted_count += self.cursor.rowcount
            self.connection.commit()
            print(f"Successfully inserted {inserted_count} movie records")
            return inserted_count
        except Error as e:
            print(f"Error inserting movies: {e}")
            if self.connection:
                self.connection.rollback()
            return 0
        finally:
            self.close()

    def get_top_movies(self, limit=10):
        """查询 Top N 电影（用于柱状图）"""
        query = """
        SELECT movie_rank, title, rating 
        FROM movies 
        WHERE movie_rank <= %s 
        ORDER BY movie_rank
        """
        try:
            self.connect()
            self.cursor.execute(query, (limit,))
            results = self.cursor.fetchall()
            return [{'movie_rank': row[0], 'title': row[1], 'rating': row[2]} for row in results]
        except Error as e:
            print(f"Error fetching top movies: {e}")
            return []
        finally:
            self.close()

    def get_genre_distribution(self, limit=5):
        """查询电影类型分布（用于饼图）"""
        query = """
        SELECT genre, COUNT(*) as count 
        FROM movies 
        GROUP BY genre 
        ORDER BY count DESC 
        LIMIT %s
        """
        try:
            self.connect()
            self.cursor.execute(query, (limit,))
            results = self.cursor.fetchall()
            return [{'genre': row[0], 'count': row[1]} for row in results]
        except Error as e:
            print(f"Error fetching genre distribution: {e}")
            return []
        finally:
            self.close()

    def get_rating_trend(self):
        """查询年份评分趋势（用于折线图）"""
        query = """
        SELECT year, AVG(rating) as avg_rating 
        FROM movies 
        GROUP BY year 
        ORDER BY year
        """
        try:
            self.connect()
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return [{'year': row[0], 'avg_rating': float(row[1])} for row in results]
        except Error as e:
            print(f"Error fetching rating trend: {e}")
            return []
        finally:
            self.close()

    def get_director_ratings(self, limit=5):
        """查询导演平均评分（用于雷达图）"""
        query = """
        SELECT director, AVG(rating) as avg_rating 
        FROM movies 
        GROUP BY director 
        ORDER BY COUNT(*) DESC 
        LIMIT %s
        """
        try:
            self.connect()
            self.cursor.execute(query, (limit,))
            results = self.cursor.fetchall()
            return [{'director': row[0], 'avg_rating': float(row[1])} for row in results]
        except Error as e:
            print(f"Error fetching director ratings: {e}")
            return []
        finally:
            self.close()