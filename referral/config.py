from sanic import Sanic
app = Sanic('myapp')

db_settings = {
    'DB_HOST': 'localhost',
    'DB_USER': 'appuser',
    'DB_PASS': 'Yamyanyo2??',
    'DB_NAME': 'appdb'
}
app.config.update(db_settings)