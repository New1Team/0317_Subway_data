from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    spark_url: str
    host_ip: str

    jdbc_jar_path: str
    data_dir: str

    mariadb_url: str
    mariadb_user: str
    mariadb_password: str
    mariadb_table: str

    mariadb_host: str
    mariadb_port: int
    mariadb_database: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()