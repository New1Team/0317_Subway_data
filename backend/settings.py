from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
  mariadb_host: str 
  file_dir: str
  host_ip:str
  spark_url:str
  jar_path:str
  db_url:str
  maria_user:str
  maria_password:str
  maria_host:str
  maria_database:str
  maria_port:int

  model_config = SettingsConfigDict(
    env_file=".env",
    env_file_encoding="utf-8",
  )

settings = Settings()
