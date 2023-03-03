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
        subscriptionStatus = request.POST['subscription_status']
        ticketId = request.POST['ticketId']

        if subscriptionStatus == "subscribed":
            try:
                obj = {
                    "name": name,
                    "email": email,
                    "subscriptionStatus": subscriptionStatus,
                    "ticketId":ticketId
                }

                data = obj
                r = requests.post('https://x5lper6q2g.execute-api.us-east-1.amazonaws.com/v1/manageemailsubscription',
                                  json=data)
                data = r.json()
                msg = data['message']
                ticketId = data['ticketId']
                messages.success(request, mark_safe(msg))

                return render(request, 'form.html', {"name": name, "email": email,"ticketId": ticketId})
            except Exception as e:
                messages.error(request, "Error: {}".format(e))
            return render(request, 'form.html', {"name": name, "email": email} )


        elif subscriptionStatus == "expired":
            try:
                obj = {
                    "name": name,
                    "email": email,
                    "subscriptionStatus": subscriptionStatus,
                    "ticketId": ticketId,
                }

                data = obj
                r = requests.post('https://x5lper6q2g.execute-api.us-east-1.amazonaws.com/v1/manageemailsubscription', json=data)
                data = r.json()
                msg = data['message']
                ticketId = data['ticketId']
                messages.success(request, mark_safe(msg))

                return render(request, 'form.html', {"name": name, "email": email,"ticketId": ticketId})
            except Exception as e:
                messages.error(request, "Error: {}".format(e))
            return render(request, 'form.html', {"name": name, "email": email})




