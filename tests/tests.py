
import datetime
from time import sleep
import factory

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.test import Client, TestCase
from django.utils import timezone


from .models import *

class UserFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = get_user_model()

class DummyPITAFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = DummyPITAModel


class TestPointInTimeModel(TestCase):
    def setUp(self):
        self.model = DummyPITAModel
        self.factory = DummyPITAFactory
        self.time = timezone.now()
        self.active_start_edge = self.factory(
            start_at=self.time
        )  # record that becomes active at the same time of the query parameter
        self.active_end_edge = self.factory(
            start_at=self.time - timezone.timedelta(days=1), end_at=self.time
        )
        self.overlap = self.factory(
            start_at=self.time - timezone.timedelta(days=1),
            end_at=self.time + timezone.timedelta(days=1),
        )
        self.no_overlap_early = self.factory(
            start_at=self.time - timezone.timedelta(days=2),
            end_at=self.time - timezone.timedelta(days=1),
        )
        self.no_overlap_late = self.factory(
            start_at=self.time + timezone.timedelta(days=1),
            end_at=self.time + timezone.timedelta(days=2),
        )

    def test_create_object_in_model(self):
        dummy = self.model.records.create(c1="Test C1", c2="", c3=5.5)
        self.assertEqual(dummy.c1, "Test C1")
        self.assertEqual(dummy.c2, "")
        self.assertEqual(dummy.c3, 5.5)

    def test_update_object(self):
        dummy = self.model.records.create(c1="Test C1", c2="", c3=5.5)
        dummy.c1 = "New C1"
        dummy.save()
        self.assertEqual(dummy.c1, self.model.records.get(pk=dummy.pk).c1)
        # check that one new row was created with the replaced dummy
        dummy_versions = self.model.records.filter(row_id=dummy.pk)
        self.assertEqual(len(dummy_versions), 2)
        # check that one version has replaced_at set
        replaced_row = dummy_versions.filter(replaced_at__isnull=False).first()
        self.assertIsNotNone(replaced_row)
        # make sure replaced_row really is a separate row in this table
        self.assertNotEqual(dummy.pk, replaced_row.pk)
        # make sure that both point to the up-to-date one via row_id
        self.assertEqual(dummy.row_id, replaced_row.row_id)
        self.assertEqual(dummy.pk, replaced_row.row_id)

        # make sure the end_at and start_at match since neither was changed in the edit
        self.assertEqual(dummy.start_at, replaced_row.start_at)
        self.assertEqual(dummy.end_at, replaced_row.end_at)

    def test_create_stores_defaults(self):
        dummy = self.model.records.create(c1="Test C1", c2="", c3=5.5)
        self.assertEqual(dummy.end_at, None)
        self.assertEqual(dummy.start_at, dummy.created_at)
        self.assertTrue(isinstance(dummy.created_at, datetime.date))
        self.assertEqual(dummy.replaced_at, None)
        self.assertEqual(dummy.modified_by, None)
        self.assertEqual(dummy.pk, dummy.row_id)

    def test_create_can_specify_certain_pita_attributes(self):
        # try specifying all PITA attributes and check to see which ones were overwritten
        pre_creation_time = timezone.now()
        created_at = pre_creation_time - timezone.timedelta(days=1)
        replaced_at = timezone.now()
        modified_by = get_user_model().objects.create(
            username="Test User", password="Test PW"
        )
        start_at = created_at
        end_at = pre_creation_time + timezone.timedelta(days=1)
        dummy = self.model.records.create(
            created_at=created_at,
            replaced_at=replaced_at,
            modified_by=modified_by,
            start_at=start_at,
            end_at=end_at,
        )

        # check that certain initial values were ignored
        self.assertNotEqual(
            dummy.created_at, created_at
        )  # the time should be set to the current time instead
        self.assertIsNone(
            dummy.replaced_at
        )  # we just created this, so it should not be replaced
        # and that some were kept
        self.assertEqual(dummy.modified_by.pk, modified_by.pk)
        self.assertEqual(dummy.start_at, start_at)
        self.assertEqual(dummy.end_at, end_at)

    def test_get_active_returns_only_all_active_rows(self):
        time = self.time
        qs = self.model.records.active(active_at=time)
        pk_list = [r.pk for r in qs]
        self.assertIn(self.active_start_edge.pk, pk_list)
        self.assertNotIn(self.active_end_edge.pk, pk_list)
        self.assertIn(self.overlap.pk, pk_list)
        self.assertNotIn(self.no_overlap_early.pk, pk_list)
        self.assertNotIn(self.no_overlap_late.pk, pk_list)

        # ensure that rows are not considered active after they are replaced
        self.overlap.replaced_at = timezone.now()
        self.overlap._save()
        qs = self.model.records.active(active_at=time)
        pk_list = [r.pk for r in qs]
        self.assertNotIn(self.overlap.pk, pk_list)

    def test_get_past_returns_all_up_to_past_date_rows(self):
        self.active_start_edge.created_at = self.time
        self.active_start_edge._save()

        self.active_end_edge.replaced_at = self.time
        self.active_end_edge._save()

        self.overlap.created_at = self.time - timezone.timedelta(days=1)
        self.overlap.replaced_at = self.time + timezone.timedelta(days=1)
        self.overlap._save()

        self.no_overlap_early.replaced_at = self.no_overlap_early.end_at
        self.no_overlap_early.created_at = self.no_overlap_early.start_at
        self.no_overlap_early._save()

        self.no_overlap_late.replaced_at = self.no_overlap_late.end_at
        self.no_overlap_late.created_at = self.no_overlap_late.start_at
        self.no_overlap_late._save()

        qs = self.model.records.version(version_at=self.time)
        pk_list = [r.pk for r in qs]
        self.assertIn(self.active_start_edge.pk, pk_list)
        self.assertNotIn(self.active_end_edge.pk, pk_list)
        self.assertIn(self.overlap.pk, pk_list)
        self.assertNotIn(self.no_overlap_early.pk, pk_list)
        self.assertNotIn(self.no_overlap_late.pk, pk_list)

    def test_replaced_past_row_stores_replaced_data(self):
        t1 = timezone.now()
        sleep(0.2)
        dummy = self.factory(c1="first")
        sleep(0.2)
        t2 = timezone.now()
        sleep(0.2)
        dummy.c1 = "second"
        dummy.save()
        sleep(0.2)
        t3 = timezone.now()
        sleep(0.2)
        dummy.c1 = "third"
        dummy.save()
        sleep(0.2)

        row_id = dummy.row_id
        initial = self.model.records.version(version_at=t1).filter(row_id=row_id)
        self.assertEqual(len(initial), 0)

        first_version = (
            self.model.records.version(version_at=t2).filter(row_id=row_id).first()
        )
        self.assertEqual(first_version.c1, "first")
        second_version = (
            self.model.records.version(version_at=t3).filter(row_id=row_id).first()
        )
        self.assertEqual(second_version.c1, "second")
        final_version = (
            self.model.records.version(version_at=timezone.now())
            .filter(row_id=row_id)
            .first()
        )
        self.assertEqual(final_version.c1, "third")

    def test_querying_date_only_uses_end_of_day(self):
        # create a dummy with a date-only start_at
        dummy = self.factory(
            start_at=timezone.localtime() - timezone.timedelta(days=1),
            end_at=timezone.localtime() + timezone.timedelta(seconds=20),
        )
        # as long as this test is not run within a minute of midnight, dummy should be active right now, but not at the end of day
        self.assertEqual(len(self.model.objects.active().filter(pk=dummy.pk)), 1)
        self.assertEqual(
            len(
                self.model.objects.active(active_at=timezone.localtime().date()).filter(
                    pk=dummy.pk
                )
            ),
            0,
        )

        # history of today should include dummy which has not been deleted yet
        self.assertEqual(
            self.model.records.version(version_at=timezone.localtime().date())
            .filter(pk=dummy.pk)
            .count(),
            1,
        )

        # but if we delete it today, it should no longer show up for today's history
        dummy.delete()
        self.assertEqual(
            self.model.records.version(version_at=timezone.localtime().date())
            .filter(pk=dummy.pk)
            .count(),
            0,
        )

    def test_delete_marks_as_inactive(self):
        dummy = self.factory(end_at=timezone.now() + timezone.timedelta(days=100))
        dummy.delete()

        # check that the record still exists, but is replaced and inactive
        _dummy = self.model.records.filter(row_id=dummy.row_id).first()
        self.assertIsNotNone(_dummy)
        self.assertIsNotNone(_dummy.replaced_at)
        self.assertLessEqual(_dummy.replaced_at, timezone.now())

    def test_updating_out_of_date_row_is_illegal(self):
        dummy = self.factory(c1="Initial")
        dummy.c1 = "Edited"
        dummy.save()

        dummy_initial = self.model.records.get(c1="Initial")
        with self.assertRaises(AssertionError):
            dummy_initial.c1 = "Illegal"
            dummy_initial.save()

        # updating the current dummy should still work fine
        dummy.c1 = "Legal"
        dummy.save()
        self.assertEqual(self.model.records.get(pk=dummy.pk).c1, "Legal")

    def test_end_must_be_after_start(self):
        now = timezone.now()
        yesterday = now - timezone.timedelta(days=1)

        # ValidationError if start_at = end_at
        with self.assertRaises(ValidationError):
            obj = DummyPITAModel.objects.create(start_at=now, end_at=now)
            obj.full_clean()

        # ValidationError if start_at > end_at
        with self.assertRaises(ValidationError):
            obj = DummyPITAModel.objects.create(start_at=now, end_at=yesterday)
            obj.full_clean()

    def test_purge_removes_all_row_versions(self):
        original_number_of_rows = DummyPITAModel.records.count()
        dummy = DummyPITAModel.objects.create(c1="Zero")
        # perform some random edits
        dummy.c1 = "First"
        dummy.save()
        dummy.c1 = "Second"
        dummy.save()
        # check that there are more rows now
        self.assertGreater(DummyPITAModel.records.count(), original_number_of_rows)

        # now purge should remove those rows
        dummy.purge()
        self.assertEqual(DummyPITAModel.records.count(), original_number_of_rows)

        # and none of the rows should have dummy's row_id
        self.assertIsNone(DummyPITAModel.records.filter(row_id=dummy.row_id).first())


class RollbackTest(TestCase):
    def setUp(self):
        self.initial_time = timezone.now()
        self.dummy = DummyPITAModel6Rollback.objects.create(
            modified_by=UserFactory(),
            c1="First",
            f1=1.5,
            time=self.initial_time,
            dummy_id=DummyPITAFactory(c1="1"),
        )

    def test_rollback_latest(self):
        dummy = self.dummy
        pk = dummy.pk

        # edit each of the fields
        dummy.c1 = "Second"
        dummy.f1 = 2.5
        dummy.time = self.initial_time + timezone.timedelta(days=1)
        dummy.dummy_id = DummyPITAFactory(c1="2")
        dummy.save()

        # another edit with one of the fields
        dummy.c1 = "Third"
        dummy.save()

        # rollback to the second version
        dummy.rollback_latest()
        self.assertEqual(dummy.c1, "Second")

        # double check that the rollback did not change the pk of the row
        self.assertEqual(dummy.pk, pk)
        dummy = DummyPITAModel6Rollback.objects.get(pk=pk)

        # rollback with fields and exclude specified
        dummy.rollback_latest(fields=["f1", "time", "c1"], exclude=["c1"])
        self.assertEqual(
            dummy.c1, "Second"
        )  # even though c1 was specified in fields, it was excluded which takes precedence
        self.assertEqual(dummy.f1, 1.5)
        self.assertEqual(dummy.time, self.initial_time)
        # dummy_id should not have been reverted
        self.assertEqual(dummy.dummy_id.c1, "2")

    def test_rollback_on_replaced_row_is_illegal(self):
        dummy = self.dummy
        dummy.c1 = "Second"
        dummy.save()
        dummy.c1 = "Third"
        dummy.save()

        stale = DummyPITAModel6Rollback.records.filter(c1="Second").first()
        with self.assertRaises(Exception):
            stale.rollback_latest()

    def test_rollback_to_at(self):
        dummy = self.dummy
        first_time = timezone.now()
        self.assertTrue(first_time >= dummy.created_at)
        self.assertEqual(
            DummyPITAModel6Rollback.records.filter(created_at__lte=first_time).count(),
            1,
        )
        sleep(0.1)

        dummy.c1 = "Second"
        dummy.save()
        second_time = timezone.now()
        sleep(0.1)

        dummy.c1 = "Third"
        dummy.save()
        third_time = timezone.now()
        sleep(0.1)

        fourth_time = timezone.now()

        # check that rolling back to third or fourth time does nothing since it has not changed since third time
        dummy.rollback_to_at(fourth_time)
        self.assertEqual(dummy.c1, "Third")
        dummy.rollback_to_at(third_time)
        self.assertEqual(dummy.c1, "Third")

        # check that we can skip over second version by rolling back to first time
        self.assertEqual(
            DummyPITAModel6Rollback.records.filter(created_at__lte=first_time).count(),
            1,
        )
        dummy.rollback_to_at(first_time)
        self.assertEqual(
            DummyPITAModel6Rollback.records.filter(row_id=dummy.row_id).count(), 1
        )

        self.assertEqual(dummy.c1, "First")

    def test_rollback_first_version_purges(self):
        dummy = self.dummy

        dummy.rollback_latest()
        self.assertEqual(DummyPITAModel6Rollback.records.all().count(), 0)

    def test_rollback_to_at_before_create_purges(self):
        dummy = self.dummy
        dummy.c1 = "Second"

        dummy.rollback_to_at(timezone.now() - timezone.timedelta(days=1))
        self.assertEqual(DummyPITAModel6Rollback.records.all().count(), 0)

    def test_rollback_does_not_affect_separate_rows(self):
        dummy = self.dummy
        dummy.c1 = "Second"
        dummy.save()
        dummy.c1 = "Third"
        dummy.save()

        # create a separate row
        separate = DummyPITAModel6Rollback.objects.create(
            modified_by=UserFactory(),
            c1="First",
            f1=1.5,
            time=self.initial_time,
            dummy_id=DummyPITAFactory(c1="1"),
        )

        # rollback the original dummy
        dummy.rollback_latest()
        # make sure the separate row was not affected
        separate = DummyPITAModel6Rollback.objects.get(pk=separate.pk)
        self.assertEqual(separate.c1, "First")


class TestPITAManagers(TestCase):
    # Note that some functionality is already tested in the TestPITAModel,
    # as it is fundamental to the model
    # This, instead, tests the specified differences between the two managers
    def setUp(self):
        self.model = DummyPITAModel
        self.factory = DummyPITAFactory
        self.time = timezone.now()

    def test_objects_returns_up_to_date_rows(self):
        d1 = self.factory(c1="First")
        # make a change and save it
        d1.c1 = "Second"
        d1.save()
        # check that there is still just one current row and it has been updated
        self.assertEqual(len(self.model.objects.all()), 1)
        self.assertEqual(self.model.objects.all().first().c1, "Second")

        # repeat at some later time
        sleep(1)
        d1.c1 = "Third"
        d1.save()
        self.assertEqual(len(self.model.objects.all()), 1)
        self.assertEqual(self.model.objects.all().first().c1, "Third")

    def test_objects_includes_replaced_rows_by_default(self):
        d1 = self.factory(c1="First")
        # make a change and save it
        d1.c1 = "Second"
        d1.save()
        self.assertEqual(len(self.model.records.all()), 2)
        # check that active filters out the replaced row
        self.assertEqual(len(self.model.records.active()), 1)

    def test_objects_is_default_manager(self):
        self.assertEqual(self.model._default_manager, self.model.objects)


class TestOneToOnePITA(TestCase):
    def setUp(self):
        self.other_a = DummyPITAFactory()
        self.other_b = DummyPITAFactory()

        self.a = DummyPITAModel4OneToOne.objects.create(one_to_one=self.other_a, c1="A")
        self.b = DummyPITAModel4OneToOne.objects.create(one_to_one=self.other_b, c1="B")

    def test_editing_does_not_cause_one_to_one_error(self):
        self.a.c1 = "A2"
        self.a.save()

        # reload to check saved value
        self.a = DummyPITAModel4OneToOne.objects.get(pk=self.a.pk)
        self.assertEqual(self.a.c1, "A2")

        # check that one_to_one is still the same
        self.assertEqual(self.a.one_to_one, self.other_a)

    def test_replaced_row_has_null_one_to_one(self):
        self.a.c1 = "A2"
        self.a.save()

        replaced_a = (
            DummyPITAModel4OneToOne.records.filter(row_id=self.a.pk)
            .exclude(pk=self.a.pk)
            .first()
        )
        self.assertIsNone(replaced_a.one_to_one)

    def test_rollback_does_not_change_one_to_one_field(self):
        self.b.one_to_one = None
        self.b.save()
        self.a.one_to_one = self.other_b
        self.a.save()
        self.a.rollback_latest()
        # make sure that the rollback did not change the one-to-one relationship
        self.assertEqual(self.a.one_to_one, self.other_b)
        print(
            "WARNING: OneToOnePITA rollback does not work when the one_to_one is set to null in an edit."
        )

class TestFrozenForeignKey(TestCase):
    def setUp(self):
        self.other = DummyPITAFactory(c1="Original A")

        self.a = DummyPITAModel7FrozenForeignKey.objects.create(frozen=self.other, c1="A")
        self.b = DummyRegularModel7FrozenForeignKey.objects.create(frozen=self.other, c1="B")

    def test_editing_does_not_cause_frozen_foreign_key_error(self):
        self.a.c1 = "A2"
        self.a.save()

        self.b.c1 = "B2"
        self.b.save()

        # reload to check saved value
        self.a = DummyPITAModel7FrozenForeignKey.objects.get(pk=self.a.pk)
        self.assertEqual(self.a.c1, "A2")

        # check that frozen_foreign_key is still the same
        self.assertEqual(self.a.frozen, self.other)

    def test_frozen_relationship_maintained_when_related_object_changes(self):
        self.other.c1 = "Changed A"
        self.other.save()

        # reload to check saved value
        self.a = DummyPITAModel7FrozenForeignKey.objects.get(pk=self.a.pk)
        self.assertEqual(self.a.frozen.c1, "Original A")

        self.b = DummyRegularModel7FrozenForeignKey.objects.get(pk=self.b.pk)
        self.assertEqual(self.b.frozen.c1, "Original A")

    def test_rollback_does_not_change_frozen_foreign_key_field(self):
        self.other.c1 = "Changed A"
        self.other.save()
        self.other.rollback_latest()

        # reload
        self.other = DummyPITAModel.objects.get(pk=self.other.pk)
        self.a = DummyPITAModel7FrozenForeignKey.objects.get(pk=self.a.pk)
        self.b = DummyRegularModel7FrozenForeignKey.objects.get(pk=self.b.pk)

        # make sure that the rollback did not change the frozen foreign key relationship
        self.assertEqual(self.a.frozen, self.other)
        self.assertEqual(self.b.frozen, self.other)