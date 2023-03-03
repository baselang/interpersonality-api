from django.shortcuts import render, redirect
import requests
from django.contrib import messages
from django.utils.safestring import mark_safe


def form_view(request):
    if request.method == 'GET':
        return render(request, 'form.html')

    elif request.method == 'POST':

        picture_data = request.POST['picture_data']
        username = request.POST['username']
        email = request.POST['email']
        issueType = request.POST['issueType']
        body = request.POST['body']

        try:
            obj = {
                "username": username,
                "email": email,
                "issueType": issueType,
                "body": body,
                "picture_data": picture_data,
                "labels": ["bug"],
            }
            #data = json.dumps(obj)
            data = obj
            r = requests.post('https://x5lper6q2g.execute-api.us-east-1.amazonaws.com/v1/creategithubissue', json=data)
            data = r.json()
            msg = data['message']
            messages.success(request, mark_safe(msg))
            return redirect('home')

        except Exception as e:
            messages.error(request, "Error: {}".format(e))
            return render(request, 'form.html',
                          {'username': username, "description": body, "email": email})
