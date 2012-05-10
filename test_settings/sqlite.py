INSTALLED_APPS = (
    'qsstats',
    'django.contrib.auth',
    'django.contrib.contenttypes'
)

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'test'
    }
}

SECRET_KEY = 'foo'