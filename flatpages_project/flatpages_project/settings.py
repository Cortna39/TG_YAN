"""Django settings for flatpages_project."""
from pathlib import Path

from celery.schedules import crontab

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

SECRET_KEY = "django-insecure-change-me"

DEBUG = True

ALLOWED_HOSTS: list[str] = ["*"]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.sites",
    "django.contrib.flatpages",
    "pages",
    "news.apps.NewsConfig",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "django.contrib.flatpages.middleware.FlatpageFallbackMiddleware",
]

ROOT_URLCONF = "flatpages_project.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    }
]

WSGI_APPLICATION = "flatpages_project.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

LANGUAGE_CODE = "ru-ru"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True

STATIC_URL = "static/"
STATICFILES_DIRS = [BASE_DIR / "static"]

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

SITE_ID = 1
LOGIN_URL = "/accounts/login/"

EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"
DEFAULT_FROM_EMAIL = "news@example.com"
ADMINS = [("Admin", "admin@example.com")]

CELERY_BROKER_URL = "redis://localhost:6379/0"
CELERY_RESULT_BACKEND = "redis://localhost:6379/0"
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_TIMEZONE = TIME_ZONE

CELERY_BEAT_SCHEDULE = {
    "weekly-news-digest": {
        "task": "news.tasks.send_weekly_news_digest",
        "schedule": crontab(day_of_week=1, hour=8, minute=0),
    },
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "require_debug_true": {
            "()": "django.utils.log.RequireDebugTrue",
        },
        "require_debug_false": {
            "()": "django.utils.log.RequireDebugFalse",
        },
    },
    "formatters": {
        "console": {
            "()": "flatpages_project.logging.ConsoleFormatter",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "general": {
            "format": "{asctime} {levelname} {module} {message}",
            "style": "{",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "errors": {
            "format": "{asctime} {levelname} {message} {pathname}\n{exc_text}",
            "style": "{",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "security": {
            "format": "{asctime} {levelname} {module} {message}",
            "style": "{",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "email": {
            "format": "{asctime} {levelname} {message} {pathname}",
            "style": "{",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "filters": ["require_debug_true"],
            "formatter": "console",
        },
        "general_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "filters": ["require_debug_false"],
            "formatter": "general",
            "filename": LOG_DIR / "general.log",
        },
        "errors_file": {
            "class": "logging.FileHandler",
            "level": "ERROR",
            "formatter": "errors",
            "filename": LOG_DIR / "errors.log",
        },
        "security_file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "security",
            "filename": LOG_DIR / "security.log",
        },
        "mail_admins": {
            "class": "django.utils.log.AdminEmailHandler",
            "level": "ERROR",
            "filters": ["require_debug_false"],
            "formatter": "email",
        },
    },
    "loggers": {
        "django": {
            "handlers": ["console", "general_file"],
            "level": "DEBUG",
            "propagate": False,
        },
        "django.request": {
            "handlers": ["errors_file", "mail_admins"],
            "level": "ERROR",
            "propagate": False,
        },
        "django.server": {
            "handlers": ["errors_file", "mail_admins"],
            "level": "ERROR",
            "propagate": False,
        },
        "django.template": {
            "handlers": ["errors_file"],
            "level": "ERROR",
            "propagate": False,
        },
        "django.db.backends": {
            "handlers": ["errors_file"],
            "level": "ERROR",
            "propagate": False,
        },
        "django.security": {
            "handlers": ["security_file"],
            "level": "INFO",
            "propagate": False,
        },
    },
}
