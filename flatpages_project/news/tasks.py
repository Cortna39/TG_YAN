from datetime import timedelta

from celery import shared_task
from django.conf import settings
from django.core.mail import send_mail
from django.utils import timezone

from .models import Category, Post


@shared_task
def send_news_notification(post_id: int) -> None:
    post = Post.objects.filter(id=post_id, type=Post.NEWS).first()
    if not post:
        return

    subscribers = (
        Category.objects.filter(postcategory__post=post)
        .values_list("subscribers__email", flat=True)
        .distinct()
    )
    recipients = [email for email in subscribers if email]
    if not recipients:
        return

    subject = f"Новая новость: {post.title}"
    message = f"{post.title}\n\n{post.text}"
    send_mail(subject, message, settings.DEFAULT_FROM_EMAIL, recipients)


@shared_task
def send_weekly_news_digest() -> None:
    since = timezone.now() - timedelta(days=7)
    posts = Post.objects.filter(type=Post.NEWS, created_at__gte=since).order_by(
        "-created_at"
    )
    if not posts.exists():
        return

    recipients = (
        Category.objects.values_list("subscribers__email", flat=True).distinct()
    )
    recipient_list = [email for email in recipients if email]
    if not recipient_list:
        return

    lines = ["Последние новости за неделю:\n"]
    for post in posts:
        lines.append(f"- {post.title}")

    message = "\n".join(lines)
    send_mail(
        "Еженедельная рассылка новостей",
        message,
        settings.DEFAULT_FROM_EMAIL,
        recipient_list,
    )
