INSTALLED_APPS = (
    'qsstats',
    'django.contrib.auth',
    'django.contrib.contenttypes'
)

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'qsstats_test',
        'USER': 'qsstats_test',
        'PASSWORD': 'qsstats_test',
    }
}

SECRET_KEY = 'foo'