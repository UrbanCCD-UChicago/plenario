from flask_bcrypt import Bcrypt

# this needs to be initialized before importing the User model. it's used there and in server.py
bcrypt = Bcrypt()

from .MetaTable import MetaTable
from .ShapeMetadata import ShapeMetadata
from .User import User
