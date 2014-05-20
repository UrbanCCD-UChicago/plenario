from wopr import create_app, make_celery
app = create_app()
celery_app = make_celery(app=app)

if __name__ == "__main__":
    app.run(debug=True, port=5001)
