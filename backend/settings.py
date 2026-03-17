from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
  mariadb_host: str 
  file_dir: str

  model_config = SettingsConfigDict(
    env_file=".env",
    env_file_encoding="utf-8",
  )

settings = Settings()
