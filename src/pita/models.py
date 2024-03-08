import datetime
from itertools import chain

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import models, transaction
from django.db.models import Deferrable, UniqueConstraint
from django.forms import ValidationError
from django.utils import timezone


def get_one_to_one_fields(model):
    """
    returns a list of the names of the one to one fields in the model
    """
    one_to_one_fields = []
    for field in model._meta.get_fields():
        if isinstance(field, models.OneToOneField):
            one_to_one_fields.append(field.name)
    return one_to_one_fields


class FrozenForeignKey(models.ForeignKey):
    """
    Use a FrozenForeignKey to a PITA model object if you want to maintain a link to the object as-is at the time of forming the link,
    even if that object changes in the future. 
    Note: in a PITA object, ManyToMany fields, OneToOne fields, and reverse ForeignKey fields will not be preserved in the past versions,
    but ForeignKeys defined in the object will be frozen.
    """
    # This class is only defined for the sake of discovering a FrozenForeignKey by a field's type, so
    # there is nothing here that needs to be implemented
    pass


class PointInTimeQuerySet(models.QuerySet):
    def active(self, active_at=None):
        """
        active_at : date datetime, default to current datetime
        Filters to return just the objects that were active as of active_at
        If active_at is a date, then it is treated as the end of that day, so that objects activated on that day are included, and those
        set inactive on that day or discluded.
        Note that this does filters out deleted objects but not out out-of-date objects. You may use current.active if you only want most current data.
        """
        if active_at is None:
            active_at = timezone.now()

        if not isinstance(active_at, datetime.datetime):
            if isinstance(active_at, datetime.date):
                # use the latest time during the provided day
                active_at = datetime.datetime.combine(active_at, datetime.time.max)
            else:
                raise ValueError("active_at must be datetime or date")

        return self.filter(
            models.Q(end_at__gt=active_at) | models.Q(end_at__isnull=True),
            start_at__lte=active_at,
        )

    def current(self):
        """
        Filters to return just the rows that represent what we currently understand to be true
        This can include inactive rows (if we currently consider them inactive), but
        will not include past versions of rows that have been edited.
        """
        return self.filter(replaced_at__isnull=True)

    def version(self, version_at):
        """
        Given a version_at : date or datetime,
        returns: all objects as they were* at version_at
        meaning, this does not return any object created later than version_at, any object deleted at/before version_at.
        If version_at is a date, then it is treated as the end of that day, so that objects created on that day are included.

        * Note that id (pk) values might be different from how they were at the past time, but row_id will be the same.
        Many-to-Many relationships depend on other models and so their history is not preserved in this one
        """
        if not isinstance(version_at, datetime.datetime):
            if isinstance(version_at, datetime.date):
                # use the latest time during the provided day
                version_at = datetime.datetime.combine(version_at, datetime.time.max)
            else:
                raise ValueError("time must be datetime or date")

        return self.filter(
            models.Q(replaced_at__gt=version_at) | models.Q(replaced_at__isnull=True),
            created_at__lte=version_at,
        )


class PointInTimeBaseManager(models.Manager):
    """
    Exposes all rows by default. Show only up-to-date ones with objects.current()
    Handle within PITA model: records
    Usage: MyModel.records.all() returns all rows (exposes PITA architecture and underlying replaced rows)
    or MyModel.records.active(time=timezone.now()) returns the most up-to-date version of objects that were active as of time.
    or MyModel.records.version(version_at) returns a queryset of all rows created but not yet replaced by time; ie. created_at <= time < replaced_at
    This is necessary as the base manager for a model in order for django to access all rows of the table.
    """

    def get_queryset(self):
        return PointInTimeQuerySet(self.model, using=self._db)

    def active(self, active_at=None):
        """
        Returns the most up-to-date version of objects that were active as of time.
        Note that a deleted object is considered out of date (inacurrate) for all time, and will not be returned
        """
        return self.get_queryset().active(active_at).current()

    def current(self):
        """
        Returns only the rows that are up-to-date
        """
        return self.get_queryset().current()

    def version(self, version_at):
        """
        Given a version_at : datetime,
        returns: all objects as they were* at version_at
        meaning, this does not return any object created later than version_at, any object deleted before version_at

        * Note that id (pk) values might be different from how they were at the past time, but row_id will be the same
        """
        # Prevent accidental related_field bugs by issuing this exception during tests/development
        if settings.DEBUG and len(self.model._meta.get_fields()) != len(
            self.model._meta.concrete_fields
        ):
            print(
                "WARNING: Related fields (OneToOne and ManyToMany) to a PITA model object might not work as expected when querying records.past. For these fields, use the current row (changes not tracked)."
            )

        return self.get_queryset().version(version_at)



class PointInTimeDefaultManager(models.Manager):
    """
    Hides implementation details of PITA architecture by only returning up-to-date rows
    Handle within PITA model: objects
    Usage: MyModel.objects.all() returns all up-to-date rows (acts like regular table architecture)
    or MyModel.objects.active(active_at=timezone.now()) returns the most up-to-date version of objects that were considered active as of active_at.
    """

    def get_queryset(self):
        return PointInTimeQuerySet(self.model, using=self._db).current()

    def current(self):
        """
        Returns only the rows that are up-to-date
        """
        return self.get_queryset()

    def active(self, active_at=None):
        """
        Returns the most up-to-date version of objects that were active as of time.
        Note that a deleted object is considered out of date (inacurrate) for all time, and will not be returned
        """
        return self.get_queryset().active(active_at)

    def copy(self, pk, exclude=()):
        """
        Copies the current instance as a new row. Does not copy ManyToMany fields.
        Specify a list of field names in exclude to set these to null in the copy.
        For instance, you should exclude any OneToOne field, as these will otherwise cause a database error.
        Model should be design to use forign key on a factored pk table if those kinds of fields are necessary.

        In an alternate design, related objects could be copied, removing the requirement of a factored pk table, but
        breaking the link between reality representations
        https://docs.djangoproject.com/en/4.2/topics/db/queries/#copying-model-instances
        """

        instance = self.current().filter(pk=pk).first()
        if instance is None:
            # we cannot copy it if the pk does not correspond with a row that is currently in the table
            return None
        else:
            instance.pk = None
            instance.id = None
            instance._state.adding = True

            for field_name in exclude:
                setattr(instance, field_name, None)

            instance._save()
            # now instance is the new row (change variable name for clarity)
            new_instance = instance
            return new_instance


class PointInTimeModel(models.Model):
    """
    Abstract Model that provides Point In Time Architecture (PITA) via Type 2 Slowly Changing Dimensions.

    Usage:
    class MyModel(PointInTimeModel):
        field1 = models.CharField(max_length=100)
        field2 = models.IntegerField()

        
    MyModel.objects.all() returns all up-to-date rows (acts like regular table architecture)
    MyModel.objects.active(active_at=timezone.now()) returns the most up-to-date version of objects that were active as of time.
    MyModel.records.version(version_at=timezone.now() - timezone.timedelta(days=10)) returns a queryset of objects as they were 10 days ago
    (as though you used MyModel.objects.all() 10 days ago*)

    * Note that id (pk) values might be different from how they were at the past time, but the row_id attribute will be the same
    * Note too that related fields are not preserved in the past versions unless frozen fields are used in the related models.

    You can use a PointInTimeModel just like a regular Django model, and access historical information whenever you need to.

    Each row describes a state that is true (or thought to be true) for some amount of time. The state may change, and
    the row becomes inactive. The row may also be changed due to a new discovery/realized mistake, and have its replaced_at set and a new row
    with the correction added, otherwise identical to the first.
    None in either end_at or replaced_at is taken to be unspecified, or +infinity

    This model managers provides methods for such functions:
    objects, the default manager can be used without client noticing the difference between this model and those with typical architectures
    objects.all() returns all up-to-date rows (acts like regular table architecture)
    objects.active(active_at=timezone.now()): returns a queryset of the most up-to-date row for each record where start_at <= active_at < end_at
    records, on the other hand, provides access into PITA properties of the model
    records.all() returns all rows (exposes PITA architecture and underlying replaced rows)
    records.active(active_at=timezone.now()): alias for objects.active
    records.version(version_at): returns a queryset of all rows created but not yet replaced by time; ie. created_at <= version_at < replaced_at

    active also works as a queryset method, so records.version(version_at).active(active_at) returns
    You can use records.active(timezone.now()) for up-to-date records of current state, and records.filter(replaced_at__isnull=True) for all up-to-date records

    purge is a method that removes all instances of the row from the database - deleting it entirely. Note that this side steps the PITA
    principle so it should be used with caution.

    rollback_latest and rollback_to_at are methods which provide the functionality of returning a row to a previous state the instance was saved at
    (either the last one or to one from
    a particular time). These permanently lose information of edits after the version they rollback to, so they should be used with caution.

    When creating an object of a PointInTimeModel, the following attributes are allowed kwargs:
    start_at: the datetime at which the record becomes active and applies to a real world state. Default = timezone.now()
    end_at: the datetime at which the record will cease to be accurate of the real world state. Default = None
    modified_by: the user associated with creating this record. Default = None

    The following attributes are not allowed as kwargs, and will be ignored if supplied:
    row_id: an internally used reference to the pk of the latest version of the record
    created_at: the datetime at which the record is created and added to the database
    replaced_at: the datetime at which this record gets modified and replaced by a more up-to-date row for the record
    A form for a PITAModel should exclude those fields in PointInTimeModel.EXCLUDE

    Note: that we chose to use row_id to connect multiple versions of the same record instead of using a separate table,
    as this provides clarity and simplicity without a significant drawback. In situations where a foreign key field is useful,
    clients can safely use one from the subclass of PointInTimeModel, as the default manager, objects, will only provide
    up-to-date versions of rows as related objects.

    Note: the surrogate id (alias pk) field is stable moving forward. For example, if you create an object and make changes to it, it's pk will stay the same.

    Note: purge and rollback each get their own permission by default, but if you are specifying permissions in the subclass, you should use
    permissions = PointInTimeModel.Meta.permissions + [your custom permissions]

    Porting an existing model to PITA:
    -Inherit PointInTimeModel
    -makemigrations (set a default for created_at and start_time; these can be timezone.now())
    -Loop over every object in the model and do the following
        obj.row_id = obj.id
        obj._save()
    """

    # fields that should not be set by user
    EXCLUDE = ("row_id", "created_at", "replaced_at", "modified_by")

    # all edits of this record will keep the same row_id which points to the most recent version
    # allowed null only until pk is known, then it is set
    row_id = models.PositiveBigIntegerField(null=True, blank=True)

    created_at = models.DateTimeField(
        blank=True
    )  # rather than using auto_now_add here, we use custom one in save() for match with replaced_at time of replaced row
    replaced_at = models.DateTimeField(null=True, blank=True)
    modified_by = models.ForeignKey(
        get_user_model(), null=True, blank=True, on_delete=models.PROTECT
    )

    start_at = models.DateTimeField(blank=True)
    end_at = models.DateTimeField(null=True, blank=True)

    objects = PointInTimeDefaultManager()
    records = PointInTimeBaseManager()

    # using transaction.atomic with the deferred unique together constraint to allow a row to be copied temporarily during an update
    @transaction.atomic
    def save(self, user=None, *args, **kwargs):
        """
        If this row does not exist, it gets saved (created) as normal.
        If this is an update instead, we copy over the unchanged row as a new row in the table (so that this current object is the one that gets changed,
        for the sake of clarity and intuitive programming that does not depend on knowledge of the implementation of this PITA)
        ie.
        >> object = MyModel.get(pk=50)
        >> object.some_attribute = new_value
        >> object.save()
        >> object.pk
            50
        >> object.some_attribute
            <new_value>
        >> MyModel.records.all().order_by('-pk').first().replaced_at
            <time of update>

        Once a row has replaced_at not None, then it cannot be changed again since it is already out-of-date
        end_at is reduced to the current time so that this row does not show up as active at any time in the future

        Note: OneToOne fields are set to null for the past version, to avoid database integrity error, and since
        it is not meaningful to have other related objects linked to an inaccessible (from their end) version of this object.
        """

        current_time = timezone.now()

        # check if this is an update
        if self.pk is not None:
            # updating a row that has already been replaced is illegal
            assert self.replaced_at is None

            # Copy the original into a new row
            original = self.copy(exclude=get_one_to_one_fields(self.__class__))

            assert (
                original is not None
            )  # this fails if we are trying to edit something that is not in the database

            # Set the replaced_at to the current time
            original.replaced_at = current_time

            # keep pointer to the current record
            original.row_id = self.pk

            original._save()

            # move reverse FrozenForeignKeys to point to the original row
            self.move_frozen_relationships(original)
            
        else:
            # this is a create, not an update, so ensure row_id is None
            self.row_id = None
            self.replaced_at = None

        # either this is a completely new row, or an updated one and we already copied the row this replaces, so we can perform regular save now
        # set default PITA attributes and ignore ones that should not be set by user
        self.created_at = current_time
        self.replaced_at = None
        self.modified_by = (
            user or self.modified_by
        )  # override the modified_by attribute only if specified in save call

        self.start_at = self.start_at or current_time
        self._save()

        # if this was a create we have to set the row_id now, after the pk has been assigned
        if self.row_id is None:
            self.row_id = self.pk

            self._save()

    def _save(self, *args, **kwargs):
        """
        A handle on the usual Model save, which bypasses the update redirect and makes an overriding permanent change to this object
        """
        return super().save(*args, **kwargs)
    
    def move_frozen_relationships(self, target):
        """
        For any FrozenForeignKey fields defined on other models pointing to self, set them to instead point to target.
        Calls _save on related PITA objects instead of save to avoid creating an artifical version change to the row.
        """
        assert self.__class__ == target.__class__

        for field in self._meta.get_fields():
            # look for FrozenForeignKey fields that point to self
            # hasattr(field, "field") is a check to see if this is a related field
            if hasattr(field, "field") and isinstance(field.field, FrozenForeignKey) and field.model == self.__class__:
                related_name = field.related_name or field.name + "_set"
                related_manager = getattr(self, related_name)
                for related in related_manager.all():
                    setattr(related, field.field.name, target)
                    # if the other model is PITA, we do not want to use PITA save, as that creates an
                    # artifical version change to the row
                    # but in reality, the target row is what that object was linking to the whole time
                    if isinstance(related, PointInTimeModel):
                        related._save()
                    else:
                        related.save()
                

    def delete(self, *args, **kwargs):
        """
        We never actually delete the row, it simply becomes replaced (by nothing) after this time
        This is useful if a record is discovered to specify a state that is never actually true in reality.
        """
        current_time = timezone.now()
        self.replaced_at = current_time

        # update the row with these changes
        self._save()

    def _delete(self, *args, **kwargs):
        """
        A handle on the usual Model delete, which bypasses the delete redirect and permenantly deletes this row.
        Typically, purge is better to use than this.
        """
        super().delete(*args, **kwargs)

    @transaction.atomic
    def purge(self, *args, **kwargs):
        """
        Permanently deletes all versions of this row from the database.
        """
        # delete all previous versions of this row
        for record in self.__class__.records.filter(row_id=self.row_id).exclude(
            pk=self.pk
        ):
            record._delete(*args, **kwargs)

        # delete this row
        self._delete(*args, **kwargs)

    def copy(self, exclude=()):
        """
        Alias for PointInTimeModelManager.copy
        copies this object if it exists and returns the new instance, or None if it did not exist in the db
        Note: you should exclude OneToOne fields to avoid database integrity error
        """
        return self.__class__.objects.copy(self.pk, exclude=exclude)

    def clean(self):
        """
        Adds vallidation check that end_at is None or else end_at > start_at,
        since a record would not make sense with end_at <= start_at

        NOTE THAT THIS SAME VALIDATION MUST BE IN ALL SERIALIZERS
        """
        super().clean()
        if self.end_at is not None and self.end_at <= self.start_at:
            raise ValidationError("end_at must be > start_at")

    @transaction.atomic
    def rollback_to_at(self, time, fields=None, exclude=None):
        """
        Rollback this object to the state it was at the provided time
        this permanently deletes rows in between the current and the one rolling back to.
        If this has not been modified at or since the time provided, this does nothing.
        If fields is specified, it only reverts those fields, and if exlcude is specified it avoids reverting any of those fields.
        """
        # this should be the most up to date version of the row
        assert (
            self.__class__.records.filter(
                row_id=self.row_id, created_at__gte=self.created_at
            ).count()
            == 1
        )

        # rollback to the latest version created at or before the time
        target = (
            self.__class__.records.filter(row_id=self.row_id, created_at__lte=time)
            .order_by("-created_at", "-pk")
            .first()
        )

        if target is None:
            # this row did not exist at the time, so purge it
            return self.purge()

        elif target.pk == self.pk:
            # this is already the version we want, so do nothing
            return self

        return self._rollback_to(target, fields=fields, exclude=exclude)

    @transaction.atomic
    def rollback_latest(self, fields=None, exclude=None):
        """
        Reverts this row to its latest previous state, permanenetly losing the current state
        If fields is specified, this only reverts those specified fields, and if exclude
        is specified, it avoids reverting those fields.
        If this row has never been changed, this simply permanently deletes it.

        """
        # get the latest previous version of this row
        previous = (
            self.__class__.records.filter(row_id=self.row_id)
            .exclude(pk=self.pk)
            .order_by("-created_at", "-pk")
            .first()
        )

        if previous is None:
            return self._delete()

        return self._rollback_to(previous, fields=fields, exclude=exclude)

    def _rollback_to(self, previous, fields=None, exclude=None):
        """
        If this is the most recent version, this reverts to the specified previous version and deletes the current one as all the
        intermediate versions.
        If fields is specified, this only reverts those specified fields, and if exclude
        is specified, it avoids reverting those fields.
        This works with the assumption that created_at always increases with each version of the row (enforced by PITA)

        Note: this does not affect many-to-many relationships, as these are not stored in this table.
        Also note, this does not rollback OneToOne relationships, since these were set to null in the past versions.
        """
        if self.pk == previous.pk:
            raise Exception("Cannot rollback to the same version of a row")

        if self.replaced_at is not None:
            raise Exception("Can only rollback a most recent version of a row")

        # invarient that should be true if replaced_at is not None
        assert self.created_at > previous.created_at

        # add one to one fields to exclude
        exclude = list(exclude or []) + get_one_to_one_fields(self.__class__)

        # copy values from the previous version to this one
        # based off of model_to_dict implementation https://docs.djangoproject.com/en/3.2/_modules/django/forms/models/
        opts = self._meta
        for f in chain(opts.concrete_fields, opts.private_fields):
            if fields is not None and f.name not in fields:
                continue
            if exclude is not None and f.name in exclude:
                continue
            if f.primary_key:
                # do not revert the pk since the most up to date version must keep the same pk consistent in PITA design
                continue
            setattr(self, f.name, getattr(previous, f.name))

        # set the replaced_at to None since this is now the most recent version
        self.replaced_at = None
        self.created_at = previous.created_at
        assert self.created_at == previous.created_at

        # move frozen relationships from previous to this, as this now represents the 
        # state that the previous version was pointing to
        previous.move_frozen_relationships(self)

        # delete all intermediate versions (which now means all that have created_at after this reverted one)
        # Note: using gte to insure that previous itself is deleted since it temporarily has the same created_at value as this
        for i in self.__class__.records.filter(created_at__gte=self.created_at, row_id=self.row_id).exclude(
            pk=self.pk
        ):
            i._delete()
        return self._save()

    class Meta:
        abstract = True
        # it shouldn't ever be possible for the row to have multiple versions with the same created_at time except for within an incomplete transaction
        constraints = [
            UniqueConstraint(
                fields=["row_id", "created_at"],
                name="%(class)s_unique_together_row_id_and_created_at",
                deferrable=Deferrable.DEFERRED,
            )
        ]
        # add purge and rollback permissions
        default_permissions = ("add", "change", "delete", "view", "purge", "rollback")
        permissions = [
            ("purge_%(class)s", "Can purge %(class)s completely from database"),
            ("rollback_%(class)s", "Can rollback %(class)s to previous version"),
        ]


class TransactionBasedModelManagerMixin:
    """
    Creates a new transaction by default for each create
    transaction_class must be specified with the model of transaction objects for this model

    For multiple objects to share the same transaction, create the transaction first and pass it in as a kwarg
    """

    transaction_class = None
    transaction_attribute = "transaction_id"

    def create(self, *args, **kwargs):
        if kwargs.get("modified_by") is None:
            raise Exception("modified_by must be specified")
        transaction = kwargs.get(self.transaction_attribute)
        if transaction is None:
            transaction = self.transaction_class.objects.create(
                modified_by=kwargs.get("modified_by")
            )
        return super().create(*args, **kwargs, transaction_id=transaction.id)

class PITATransactionMixin:
    """
    Deleting a transaction connected to PITA objects can lead to uninteded consequences and
    may break the PITA architecture. This mixin prevents deletion of a PITATransaction object,
    though it offers the _delete method to override this for rare administrator only exceptions.

    Instead of deleting a PITATransaction, it is recommended to delete the objects that are part of it.
    It only ever makes sense to delete a PITATransaction if it has no objects (even deleted PITA ones) linked to it.
    """

    def delete(self, *args, **kwargs):
        raise Exception(
            "Warning: do not delete a PITATransaction that has objects linked to it. If delete is necessary, use _delete to override this."
        )

    def _delete(self, *args, **kwargs):
        return super().delete()
