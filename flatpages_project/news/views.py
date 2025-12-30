from django.urls import reverse_lazy
from django.views.generic import CreateView, DeleteView, UpdateView

from .forms import PostForm
from .models import Post


class PostTypeMixin:
    post_type = None
    type_label = ""
    success_url = reverse_lazy("news:news-create")

    def get_queryset(self):
        return Post.objects.filter(type=self.post_type)

    def form_valid(self, form):
        form.instance.type = self.post_type
        return super().form_valid(form)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["type_label"] = self.type_label
        return context


class NewsCreateView(PostTypeMixin, CreateView):
    model = Post
    form_class = PostForm
    template_name = "news/post_form.html"
    post_type = Post.NEWS
    type_label = "новость"
    success_url = reverse_lazy("news:news-create")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["action_label"] = "Создать"
        return context


class NewsUpdateView(PostTypeMixin, UpdateView):
    model = Post
    form_class = PostForm
    template_name = "news/post_form.html"
    post_type = Post.NEWS
    type_label = "новость"
    success_url = reverse_lazy("news:news-create")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["action_label"] = "Сохранить"
        return context


class NewsDeleteView(PostTypeMixin, DeleteView):
    model = Post
    template_name = "news/post_confirm_delete.html"
    post_type = Post.NEWS
    type_label = "новость"
    success_url = reverse_lazy("news:news-create")


class ArticleCreateView(PostTypeMixin, CreateView):
    model = Post
    form_class = PostForm
    template_name = "news/post_form.html"
    post_type = Post.ARTICLE
    type_label = "статья"
    success_url = reverse_lazy("news:article-create")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["action_label"] = "Создать"
        return context


class ArticleUpdateView(PostTypeMixin, UpdateView):
    model = Post
    form_class = PostForm
    template_name = "news/post_form.html"
    post_type = Post.ARTICLE
    type_label = "статья"
    success_url = reverse_lazy("news:article-create")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["action_label"] = "Сохранить"
        return context


class ArticleDeleteView(PostTypeMixin, DeleteView):
    model = Post
    template_name = "news/post_confirm_delete.html"
    post_type = Post.ARTICLE
    type_label = "статья"
    success_url = reverse_lazy("news:article-create")
