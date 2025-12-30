from django.urls import path

from . import views

app_name = "news"

urlpatterns = [
    path("news/create/", views.NewsCreateView.as_view(), name="news-create"),
    path("news/<int:pk>/edit/", views.NewsUpdateView.as_view(), name="news-edit"),
    path("news/<int:pk>/delete/", views.NewsDeleteView.as_view(), name="news-delete"),
    path("articles/create/", views.ArticleCreateView.as_view(), name="article-create"),
    path("articles/<int:pk>/edit/", views.ArticleUpdateView.as_view(), name="article-edit"),
    path("articles/<int:pk>/delete/", views.ArticleDeleteView.as_view(), name="article-delete"),
]
