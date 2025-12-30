from django.db.models.signals import post_save
from django.dispatch import receiver

from .models import Post
from .tasks import send_news_notification


@receiver(post_save, sender=Post)
def notify_subscribers_on_news_create(sender, instance: Post, created: bool, **kwargs):
    if created and instance.type == Post.NEWS:
        send_news_notification.delay(instance.id)
