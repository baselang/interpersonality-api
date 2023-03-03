from django.shortcuts import render, redirect
import requests
from django.contrib import messages
from django.utils.safestring import mark_safe


def form_view(request):
    if request.method == 'GET':
        return render(request, 'form.html')

    elif request.method == 'POST':
        
        name = request.POST['name']
        email = request.POST['email']        
        body = request.POST['body']
        tagType = request.POST['tagType']
        supportPriority = request.POST['priority']

        try:
            obj = {
                "name": name,
                "email": email,
                "body": body,
                "tagType":tagType,
                "supportPriority":supportPriority
            }
            #data = json.dumps(obj)
            data = obj
            r = requests.post('https://x5lper6q2g.execute-api.us-east-1.amazonaws.com/v1/profilessupportticket', json=data)
            data = r.json()
            msg = data['message']
            messages.success(request, mark_safe(msg))
            return redirect('home')

        except Exception as e:
            messages.error(request, "Error: {}".format(e))
            return render(request, 'form.html',
                          {'name': name, "description": body, "email": email})
