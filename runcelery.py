from wopr import create_app
from wopr.tasks import celery_app
app = create_app()

if __name__ == "__main__":
    with app.app_context():
        celery_app.start()
