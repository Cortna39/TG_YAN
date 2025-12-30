from django import forms

from .models import Post


class PostForm(forms.ModelForm):
    class Meta:
        model = Post
        fields = ["author", "categories", "title", "text", "rating"]
        widgets = {
            "text": forms.Textarea(attrs={"rows": 6}),
        }
