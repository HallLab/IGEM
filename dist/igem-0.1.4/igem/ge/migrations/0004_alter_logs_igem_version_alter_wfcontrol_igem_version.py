# Generated by Django 4.2.1 on 2023-05-23 14:24

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ge", "0003_alter_logs_igem_version_alter_wfcontrol_igem_version"),
    ]

    operations = [
        migrations.AlterField(
            model_name="logs",
            name="igem_version",
            field=models.CharField(default="0.1.3", max_length=15),
        ),
        migrations.AlterField(
            model_name="wfcontrol",
            name="igem_version",
            field=models.CharField(default="0.1.3", max_length=15),
        ),
    ]