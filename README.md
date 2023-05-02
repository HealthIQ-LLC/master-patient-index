# master-patient-index
Enterprise Master Patent Index

#Requirements
Developed with Docker version 23.0.5, build bc4487a  
Python 3.10  
Flask 2.2.2  

# Basic Docker Command Pastebase
sudo docker compose -f docker-compose.yml up -d --build  
sudo docker compose exec web python manage.py create_db  
sudo docker compose -f docker-compose.yml down -v  
sudo docker compose exec db psql --username=compadre --dbname=compadre  

# Hidden Files

## .env.dev
FLASK_APP=project/__init__.py  
FLASK_DEBUG=1  
DATABASE_URL=postgresql://<username>:<password>!@db:5432/<database>  
SQL_HOST=db  
SQL_PORT=5432  
DATABASE=postgres  
APP_FOLDER=/usr/src/app  

## .env.prod
FLASK_APP=project/__init__.py  
FLASK_DEBUG=1  
DATABASE_URL=postgresql://<username>:<password>!@db:5432/<database>  
SQL_HOST=db  
SQL_PORT=5432  
DATABASE=postgres  
APP_FOLDER=/home/app/web  

## .env.prod.db
POSTGRES_USER=<username>  
POSTGRES_PASSWORD=<password>  
POSTGRES_DB=<database>  
