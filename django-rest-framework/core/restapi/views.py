from django.contrib.auth.models import Group, User
from rest_framework import permissions, viewsets
from core.restapi.serializers import GroupSerializer, UserSerializer
from .models import Task
from .serializers import TaskSerializer

class UserViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows users to be viewed or edited.
    """
    queryset = User.objects.all().order_by('-date_joined')
    serializer_class = UserSerializer
    permission_classes = [permissions.IsAuthenticated]


class GroupViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """
    queryset = Group.objects.all().order_by('name')
    serializer_class = GroupSerializer
    permission_classes = [permissions.IsAuthenticated]

class TaskViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows tasks to be viewed or edited.
    """
    queryset= Task.objects.all().order_by('created_at')
    serializer_class = TaskSerializer
    permission_classes = [permissions.IsAuthenticated]
