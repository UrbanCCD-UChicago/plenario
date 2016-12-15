if __name__ == "__main__":
    from plenario import create_app
    application = create_app()
    application.run(host="0.0.0.0")
