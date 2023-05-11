# Generated by Django 4.1.5 on 2023-05-01 19:02

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ge", "0004_wordterm_ge_wordterm_term_id_9bd525_idx"),
    ]

    operations = [
        migrations.CreateModel(
            name="Logs",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("process", models.CharField(max_length=65)),
                ("igem_version", models.CharField(default="0.1.0", max_length=15)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("description", models.TextField(blank=True, default=None, null=True)),
            ],
        ),
        migrations.AlterField(
            model_name="wfcontrol",
            name="source_file_version",
            field=models.CharField(blank=True, default="", max_length=500, null=True),
        ),
    ]