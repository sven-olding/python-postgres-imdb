from dotenv import load_dotenv
import os
import psycopg
import gzip
import shutil
import sys
import getopt

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

TSV_FILES = [
            "name.basics.tsv",
            "title.akas.tsv",
            "title.basics.tsv",
            "title.crew.tsv",
            "title.episode.tsv",
            "title.principals.tsv",
            "title.ratings.tsv"
        ]

def main(argv):
    print(f"Using {DB_NAME} on {DB_HOST}:{DB_PORT} as {DB_USER}")
    
    db_drop = False
    try:
        opts, _ = getopt.getopt(argv[1:], "d", ["drop"])
        for opt, _ in opts:
            if opt in ("-d", "--drop"):
                drop_and_create_database()
                db_drop = True
                print("Database dropped and created.")
                
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)
    
    print("unpacking gz files...")
    for tsv_file in TSV_FILES:
        unpack_gz(f"imdb_data/{tsv_file}.gz")
        
    if db_drop:
        create_tables()
        
    import_data()
    
    
def drop_and_create_database():
    with psycopg.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
            cur.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"Database {DB_NAME} created successfully.")
    
def unpack_gz(file_path):
    if not os.path.exists(file_path):
        return
    with gzip.open(file_path, 'rb') as f_in:
        with open(file_path[:-3], 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(file_path) 
    

def create_tables():
    with psycopg.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, dbname=DB_NAME) as conn:
        with conn.cursor() as cur:
            # Create tables
            cur.execute("""
                CREATE TABLE title_ratings (tconst VARCHAR(10),average_rating NUMERIC,num_votes integer);
                CREATE TABLE name_basics (nconst varchar(10), primaryName text, birthYear smallint, deathYear smallint, primaryProfession text, knownForTitles text );
                CREATE TABLE title_akas (titleId TEXT, ordering INTEGER, title TEXT, region TEXT, language TEXT, types TEXT, attributes TEXT, isOriginalTitle BOOLEAN);
                CREATE TABLE title_basics (tconst TEXT, titleType TEXT, primaryTitle TEXT, originalTitle TEXT, isAdult BOOLEAN, startYear SMALLINT, endYear SMALLINT, runtimeMinutes INTEGER, genres TEXT);
                CREATE TABLE title_crew (tconst TEXT, directors TEXT, writers TEXT);
                CREATE TABLE title_episode (const TEXT, parentTconst TEXT, seasonNumber TEXT, episodeNumber TEXT);
                CREATE TABLE title_principals (tconst TEXT, ordering INTEGER, nconst TEXT, category TEXT, job TEXT, characters TEXT);
            """)
            print("Tables created successfully.")

            conn.commit()
    
def import_data():
    with psycopg.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, dbname=DB_NAME) as conn:
        with conn.cursor() as cur:
            for tsv_file in TSV_FILES:
                split = tsv_file.split('.')
                table_name = split[0] + "_" + split[1] 
                file_path = f"imdb_data/{tsv_file}"
                
                with open(file_path, "r", encoding="utf-8") as f:
                    with cur.copy(f"COPY {table_name} FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\t', NULL '\\N', HEADER true)") as copy:
                        copy.write(f.read())
                    print(f"Data from {tsv_file} imported into {table_name}.")
                    conn.commit()
                    
            print("All data imported successfully.")


if __name__ == "__main__":
    main(sys.argv)