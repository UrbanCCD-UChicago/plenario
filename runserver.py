import logging
from plenario import create_app
logging.basicConfig()

application = create_app()

if __name__ == "__main__":
    application.run(debug=True)

# import redis
# import logging
# import datetime
# from flask import Flask, request
# from flask.ext.cache import Cache
# from plenario.settings import CACHE_CONFIG, REDIS_HOST
#
# cache = Cache(config=CACHE_CONFIG)
# RESPONSE_LIMIT = 1000
# CACHE_TIMEOUT = 60*60*6
#
#
#
# def make_cache_key(*args, **kwargs):
#     path = request.path
#     args = str(hash(frozenset(request.args.items())))
#     return (path + args).encode('utf-8')
#
#
# # print a nice greeting.
# def say_hello(username = "World"):
#     return '<p>Hello %s!</p>\n' % username
#
# # some bits of text for the page.
# header_text = '''
#     <html>\n<head> <title>EB Flask Test</title> </head>\n<body>'''
# instructions = '''
#     <p><em>Hint</em>: This is a RESTful web service! Append a username
#     to the URL (for example: <code>/Thelonious</code>) to say hello to
#     someone specific.</p>\n'''
# home_link = '<p><a href="/">Back</a></p>\n'
# footer_text = '</body>\n</html>'
#
# # EB looks for an 'application' callable by default.
# application = Flask(__name__)
# application.debug = True
# cache.init_app(application)
#
# # add a rule for the index page.
# application.add_url_rule('/', 'index', (lambda: header_text +
#     say_hello() + instructions + footer_text))
#
# # add a rule when the page is accessed with a name appended to the site
# # URL.
# # application.add_url_rule('/hello/<username>', 'hello', (lambda username:
# #     header_text + say_hello(username) + home_link + footer_text))
#
#
# @cache.cached(timeout=CACHE_TIMEOUT)
# def try_to_import_settings():
#     return str(datetime.datetime.now())
#     # from plenario.database import app_engine
#     # conn = app_engine.connect()
#     # conn.close()
#     # print 'Trying to connect to host', REDIS_HOST
#     # try:
#     #     redis_client = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)
#     # except Exception as e:
#     #     print e.message
#     #     return e.message
#
#     # if redis_client.ping():
#     #     print 'Home stretch'
#     #     return str(datetime.datetime.now())
#     # else:
#     #     return "Redis ain't flying bruv."
#
# application.add_url_rule('/willz', 'settings_kludge', try_to_import_settings)
#
# # run the app.
# if __name__ == "__main__":
#     # Setting debug to True enables debug output. This line should be
#     # removed before deploying a production app.
#     # application.debug = True
#     # cache.init_app(application)
#     application.run(debug=True)
