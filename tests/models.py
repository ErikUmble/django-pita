from django.db import models
from django.contrib.auth import get_user_model

from pita.models import (PointInTimeModel, PointInTimeModelPK, DateOnlyPITAMixin, FrozenForeignKey)

class DummyPITAModel(PointInTimeModel):
    """
    A dummy model for the purpose of testing the PointInTimeModel's abstract properties
    """

    c1 = models.CharField(max_length=64)
    c2 = models.CharField(max_length=256, blank=True, default="")
    c3 = models.FloatField(null=True, blank=True)


class DummyPITAModel2PK(PointInTimeModelPK):
    pass


class DummyPITAModel2State(PointInTimeModel):
    """
    For testing the PointInTimeModelPK abstract properties
    """

    real_pk = models.ForeignKey(
        DummyPITAModel2PK, on_delete=models.CASCADE, related_name="record_set"
    )
    value = models.IntegerField()


class DummyPITAModel3Dateonly(DateOnlyPITAMixin, PointInTimeModel):
    """
    For testing the DateOnlyPITAMixin
    """

    pass


class DummyPITAModel4OneToOne(PointInTimeModel):
    """
    For testing OneToOne fields
    """

    c1 = models.CharField(max_length=64, blank=True, default="")
    one_to_one = models.OneToOneField(
        DummyPITAModel, on_delete=models.CASCADE, null=True, blank=True
    )


class DummyPITAModel6Rollback(PointInTimeModel):
    """
    contains a variety of kinds of fields to test rollback on
    """

    now_add = models.DateTimeField(auto_now_add=True)
    modified_by = models.ForeignKey(get_user_model(), on_delete=models.PROTECT)
    now = models.DateTimeField(auto_now=True)
    c1 = models.CharField(max_length=64, blank=True, default="")
    f1 = models.FloatField(null=True, blank=True)
    dummy_id = models.ForeignKey(
        DummyPITAModel, on_delete=models.PROTECT, null=True, blank=True
    )
    time = models.DateTimeField(null=True, blank=True)

class DummyPITAModel7FrozenForeignKey(PointInTimeModel):
    """
    For testing FrozenForeignKey fields
    """
    c1 = models.CharField(max_length=64, blank=True, default="")
    frozen = FrozenForeignKey(DummyPITAModel, on_delete=models.PROTECT, null=True, blank=True)

class DummyRegularModel7FrozenForeignKey(models.Model):
    """
    For testing FrozenForeignKey fields
    """
    c1 = models.CharField(max_length=64, blank=True, default="")
    frozen = FrozenForeignKey(DummyPITAModel, on_delete=models.PROTECT, null=True, blank=True, related_name="needs_frozen")