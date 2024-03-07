from abc import ABC, abstractmethod

from rest_framework import permissions, status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated, DjangoModelPermissions
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from utils import (
    date_or_datetime,
    datetime_from_web,
)

class DjangoModelPermissionsStrict(DjangoModelPermissions):
    """
    Modification of the original DjangoModelPermissions to enforce django view permission for GET requests
    Note: this is necessary as of stable version 3.14.0, but a recent patch has made this the new default for DjangoModelPermissions,
    so it might not be necessary for your version.
    """

    perms_map = {
        "GET": ["%(app_label)s.view_%(model_name)s"],
        "OPTIONS": [],
        "HEAD": [],
        "POST": ["%(app_label)s.add_%(model_name)s"],
        "PUT": ["%(app_label)s.change_%(model_name)s"],
        "PATCH": ["%(app_label)s.change_%(model_name)s"],
        "DELETE": ["%(app_label)s.delete_%(model_name)s"],
    }


def get_pita_permissions_class(model_class):
    """
    returns a class to handle default permissions on a PITA model, given that model
    """

    class PointInTimeModelPermissions(permissions.BasePermission):
        """
        handles permission checks for custom PITA actions (purge and rollback) to avoid the
        default requirement based on the http method
        """

        def has_permission(self, request, view):
            if view.action == "purge":
                return request.user.has_perm(
                    model_class._meta.app_label
                    + ".purge_"
                    + model_class._meta.model_name
                )
            elif view.action == "rollback":
                return request.user.has_perm(
                    model_class._meta.app_label
                    + ".rollback_"
                    + model_class._meta.model_name
                )

            return DjangoModelPermissionsStrict().has_permission(request, view)

    return PointInTimeModelPermissions


class PointInTimeModelViewSet(ModelViewSet, ABC):
    """
    This Abstract Class handles PITA specifics including defaulting to objects manager and active rows if
    time is specified, provides hisotry endpoint for table at past state,
    and sets user to modified_by on update and create
    This should generally be used instead of base ModelViewSet for any PITA model

    Note: It is REQUIRED that subclasses specify a filter_queryset method. This can
    simply return the same queryset if further filtering based on the request is not needed.

    This provides actions for purge and for rollback, which by default requre the purge_modelname or rollback_modelname permissions
    respectively. You can require different conditions or disable them altogether by overriding get_permissions.

    If you override get_permissions, you can subclass PITAModelViewSet.PITAPermissions to get default behavior: required default
    permissions based on http method (DjangoModelPermissionsStrict) and purge and rollback permissions based on the permissions.
    Example of how to do that:
    class MyViewSet(PointInTimeModelViewSet):
        model_class = MyPITAModelName
        class CustomPermissions(get_pita_permissions_class(MyPITAModelName), permissions.BasePermission):
            def has_permission(self, request, view):
                # handle your custom situations here
                return super().has_permission(request, view) # this handles PITA actions and defaults the rest to DjangoModelPermissionsStrict

        def get_permissions(self):
            permission_classes = [IsAuthenticated, self.CustomPermissions]
            return [permission() for permission in permission_classes]

        ... other methods ...
    """

    model_class = None

    @abstractmethod
    def filter_queryset(self, queryset, *args, **kwargs):
        """
        This will be called for any queryset used by the class.
        This is where a subclass should filter the queryset based on the request.
        """
        pass

    def get_queryset(self):
        """
        returns the default manager rows - hiding PITA implementation
        if active_at is specified in the GET parameters, this returns the objects
        active at that time
        and if version_at is specified in the GET parameters, this returns 
        the objects as they were in the database at that time
        """
        if self.model_class is None:
            raise Exception("model must be specified for PointInTimeViewset")

        qs = self.model_class.objects.all()

        # If time is specified, filter to the active fields
        if self.request.method == "GET":
            # querying past versions is only allowed in GET since these cannot be modified
            active_at = date_or_datetime(datetime_from_web(self.request.GET.get("active_at")))
            version_at = date_or_datetime(datetime_from_web(self.request.GET.get("version_at")))

            if version_at is not None:
                qs = self.model_class.records.past(past_time=version_at)

            if active_at is not None:
                qs = qs.active(time=active_at)
            

        return self.filter_queryset(qs)

    def get_model_class(self):
        """
        returns the model class for this viewset
        """
        if self.model_class is None:
            raise Exception("model must be specified for PITAViewset")
        return self.model_class

    def get_permissions(self):
        """
        Use PITAPermissions
        """
        permission_classes = [
            IsAuthenticated,
            get_pita_permissions_class(self.get_model_class()),
        ]
        return [permission() for permission in permission_classes]

    def perform_create(self, serializer):
        if self.request is not None and self.request.user is not None:
            serializer.save(modified_by=self.request.user)
        else:
            serializer.save()

    def perform_update(self, serializer):
        # Needs unit test
        if self.request is not None and self.request.user is not None:
            serializer.save(modified_by=self.request.user)
        else:
            serializer.save()

    @action(detail=True, methods=["delete"])
    def purge(self, request, *args, **kwargs):
        """
        Purges the object from the database if that is allowed for the model
        """

        instance = self.get_object()
        instance.purge()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=True, methods=["post"])
    def rollback(self, request, *args, **kwargs):
        """
        Rolls back the object to the state it was in at the time specified in the POST data
        or just to the latest version if time is not specified.
        The fields and exclude parameters are optional and can be used to specify which fields should be rolled back.
        If specified, time should be in iso format and fields and exclude should be iterables of field names if specified.
        """

        time = date_or_datetime(datetime_from_web(request.data.get("time")))
        fields = request.data.get("fields")
        exclude = request.data.get("exclude")

        # validate that fields and exclude are iterable if specified
        if fields is not None and not hasattr(fields, "__iter__"):
            return Response(
                "fields must be iterable", status=status.HTTP_400_BAD_REQUEST
            )

        if exclude is not None and not hasattr(exclude, "__iter__"):
            return Response(
                "exclude must be iterable", status=status.HTTP_400_BAD_REQUEST
            )

        instance = self.get_object()
        if time is None:
            instance.rollback_latest(fields=fields, exclude=exclude)
        else:
            instance.rollback_to_at(time, fields=fields, exclude=exclude)
        try:
            instance = self.get_object()
        except Exception as e:
            # catch situation where object was deleted in rollback
            instance = None

        if instance is None:
            return Response(status=status.HTTP_204_NO_CONTENT)

        serializer = self.get_serializer(instance)
        return Response(serializer.data, status=status.HTTP_200_OK)

