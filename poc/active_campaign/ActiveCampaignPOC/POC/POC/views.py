import json
from django.shortcuts import render, redirect
import requests
from django.contrib import messages
from django.utils.safestring import mark_safe


def home(request):
    if request.method == "POST":
        first_name = request.POST['first_name']
        last_name = request.POST['last_name']
        email = request.POST['email']
        language = request.POST['language']
        gender = request.POST['gender']
        age = request.POST['age']
        style_assignment = request.POST['style_assignments']
        user_id = request.POST['uid']
        compatability_guides = request.POST['compatibility']
        couples_guides = request.POST['couples']
        product_8_dates = request.POST['product_8_dates']
        is_8_dates = request.POST['exampleRadios']
        partners_first_name = request.POST['partners_first_name']
        partners_gender = request.POST['partners_gender']
        partners_user_id = request.POST['partners_uid']
        partner_style_assignment = request.POST['partners_style_assignments']

        if is_8_dates == 'option1':
            is_8_dates = 'Yes'
        else:
            is_8_dates = 'No'

        try:
            obj = {
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'language': language,
                'gender': gender,
                'age':age,
                'style_assignment': style_assignment,
                'user_id': user_id,
                'compatability_guides': compatability_guides,
                'couples_guides': couples_guides,
                'product_8_dates': product_8_dates,
                'is_8_dates': is_8_dates,
                'partners_first_name': partners_first_name,
                'partners_gender': partners_gender,
                'partners_user_id': partners_user_id,
                'partner_style_assignment': partner_style_assignment
            }

            r = requests.post('https://x5lper6q2g.execute-api.us-east-1.amazonaws.com/v1/addcontactintoactiveaampaign', json=obj)
            data = r.json()
            msg = data['message']
            contact_id = data['contact_id']
            messages.success(request, mark_safe(msg))
            fieldValues = data['fieldValues']
            ageFieldValue = fieldValues['ageFieldValue']
            genderFieldValue = fieldValues['genderFieldValue']
            languageContactTag = fieldValues['languageContactTag']
            styleAssignmentContactTag = fieldValues['styleAssignmentContactTag']
            userIdFieldValue = fieldValues['userIdFieldValue']
            compatabilityGuidesFieldValue = fieldValues['compatabilityGuidesFieldValue']
            couplesGuidesFieldValue = fieldValues['couplesGuidesFieldValue']
            product8DatesFieldValue = fieldValues['product8DatesFieldValue']
            partnersFirstNameFieldValue = fieldValues['partnersFirstNameFieldValue']
            partnersGenderFieldValue = fieldValues['partnersGenderFieldValue']
            partnersUserIdFieldValue = fieldValues['partnersUserIdFieldValue']
            partnerStyleAssignmentContactTag = fieldValues['partnerStyleAssignmentContactTag']

            return render(request, 'update_form.html', {
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'language': language,
                'gender': gender,
                'age':age,
                'style_assignment': style_assignment,
                'user_id': user_id,
                'compatability_guides': compatability_guides,
                'couples_guides': couples_guides,
                'product_8_dates': product_8_dates,
                'is_8_dates': is_8_dates,
                'partners_first_name': partners_first_name,
                'partners_gender': partners_gender,
                'partners_user_id': partners_user_id,
                'partner_style_assignment': partner_style_assignment,

                "contact_id": contact_id,
                'ageFieldValue': ageFieldValue,
                "genderFieldValue": genderFieldValue,
                "languageContactTag": languageContactTag,
                "styleAssignmentContactTag": styleAssignmentContactTag,
                "userIdFieldValue": userIdFieldValue,
                "compatabilityGuidesFieldValue": compatabilityGuidesFieldValue,
                "couplesGuidesFieldValue": couplesGuidesFieldValue,
                "product8DatesFieldValue": product8DatesFieldValue,
                "partnersFirstNameFieldValue": partnersFirstNameFieldValue,
                "partnersGenderFieldValue": partnersGenderFieldValue,
                "partnersUserIdFieldValue": partnersUserIdFieldValue,
                "partnerStyleAssignmentContactTag": partnerStyleAssignmentContactTag
            })

        except Exception as e:
            messages.error(request, "Error: {}".format(e))
            return redirect('home')

    return render(request, 'save_form.html')


def update(request):
    if request.method == 'POST' and 'delete' in request.POST:
        first_name = request.POST['first_name']
        last_name = request.POST['last_name']
        email = request.POST['email']
        language = request.POST['language']
        gender = request.POST['gender']
        age = request.POST['age']
        style_assignment = request.POST['style_assignments']
        user_id = request.POST['uid']
        compatability_guides = request.POST['compatibility']
        couples_guides = request.POST['couples']
        product_8_dates = request.POST['product_8_dates']
        is_8_dates = request.POST['exampleRadios']
        partners_first_name = request.POST['partners_first_name']
        partners_gender = request.POST['partners_gender']
        partners_user_id = request.POST['partners_uid']
        partner_style_assignment = request.POST['partners_style_assignments']
        genderFieldValue = request.POST['genderFieldValue']
        ageFieldValue = request.POST['ageFieldValue']
        languageContactTag = request.POST['languageContactTag']
        styleAssignmentContactTag = request.POST['styleAssignmentContactTag']
        userIdFieldValue = request.POST['userIdFieldValue']
        compatabilityGuidesFieldValue = request.POST['compatabilityGuidesFieldValue']
        couplesGuidesFieldValue = request.POST['couplesGuidesFieldValue']
        product8DatesFieldValue = request.POST['product8DatesFieldValue']
        partnersFirstNameFieldValue = request.POST['partnersFirstNameFieldValue']
        partnersGenderFieldValue = request.POST['partnersGenderFieldValue']
        partnersUserIdFieldValue = request.POST['partnersUserIdFieldValue']
        partnerStyleAssignmentContactTag = request.POST['partnerStyleAssignmentContactTag']
        contact_id = request.POST['contact_id']

        if is_8_dates == 'option1':
            is_8_dates = 'Yes'
        else:
            is_8_dates = 'No'

        try:
            obj = {
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'language': language,
                'gender': gender,
                'age': age,
                'style_assignment': style_assignment,
                'user_id': user_id,
                'compatability_guides': compatability_guides,
                'couples_guides': couples_guides,
                'product_8_dates': product_8_dates,
                'is_8_dates': is_8_dates,
                'partners_first_name': partners_first_name,
                'partners_gender': partners_gender,
                'partners_user_id': partners_user_id,
                'partner_style_assignment': partner_style_assignment,
                'contact_id':contact_id,
                'fieldValues': {
                    "genderFieldValue": genderFieldValue,
                    'ageFieldValue': ageFieldValue,
                    "languageContactTag": languageContactTag,
                    "styleAssignmentContactTag": styleAssignmentContactTag,
                    "userIdFieldValue": userIdFieldValue,
                    "compatabilityGuidesFieldValue": compatabilityGuidesFieldValue,
                    "couplesGuidesFieldValue": couplesGuidesFieldValue,
                    "product8DatesFieldValue": product8DatesFieldValue,
                    "partnersFirstNameFieldValue": partnersFirstNameFieldValue,
                    "partnersGenderFieldValue": partnersGenderFieldValue,
                    "partnersUserIdFieldValue": partnersUserIdFieldValue,
                    "partnerStyleAssignmentContactTag":partnerStyleAssignmentContactTag
                }
            }
            r = requests.delete('https://x5lper6q2g.execute-api.us-east-1.amazonaws.com/v1/deletecontactintoactiveaampaign', json=obj)
            data = r.json()
            msg = data['message']
            messages.success(request, mark_safe(msg))
            return redirect('home')
        except Exception as e:
            messages.error(request, "Error: {}".format(e))
            return render(request, 'update_form.html', {
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'language': language,
                'gender': gender,
                'age': age,
                'style_assignment': style_assignment,
                'user_id': user_id,
                'compatability_guides': compatability_guides,
                'couples_guides': couples_guides,
                'product_8_dates': product_8_dates,
                'is_8_dates': is_8_dates,
                'partners_first_name': partners_first_name,
                'partners_gender': partners_gender,
                'partners_user_id': partners_user_id,
                'partner_style_assignment': partner_style_assignment,
                'contact_id': contact_id,
                "genderFieldValue": genderFieldValue,
                "languageContactTag": languageContactTag,
                'ageFieldValue': ageFieldValue,
                "styleAssignmentContactTag": styleAssignmentContactTag,
                "userIdFieldValue": userIdFieldValue,
                "compatabilityGuidesFieldValue": compatabilityGuidesFieldValue,
                "couplesGuidesFieldValue": couplesGuidesFieldValue,
                "product8DatesFieldValue": product8DatesFieldValue,
                "partnersFirstNameFieldValue": partnersFirstNameFieldValue,
                "partnersGenderFieldValue": partnersGenderFieldValue,
                "partnersUserIdFieldValue": partnersUserIdFieldValue,
                "partnerStyleAssignmentContactTag": partnerStyleAssignmentContactTag
            })
    if request.method == "POST" and 'update' in request.POST:
        first_name = request.POST['first_name']
        last_name = request.POST['last_name']
        email = request.POST['email']
        language = request.POST['language']
        gender = request.POST['gender']
        age = request.POST['age']
        style_assignment = request.POST['style_assignments']
        user_id = request.POST['uid']
        compatability_guides = request.POST['compatibility']
        couples_guides = request.POST['couples']
        product_8_dates = request.POST['product_8_dates']
        is_8_dates = request.POST['exampleRadios']
        partners_first_name = request.POST['partners_first_name']
        partners_gender = request.POST['partners_gender']
        partners_user_id = request.POST['partners_uid']
        partner_style_assignment = request.POST['partners_style_assignments']
        contact_id = request.POST['contact_id']
        genderFieldValue = request.POST['genderFieldValue']
        ageFieldValue = request.POST['ageFieldValue']
        languageContactTag = request.POST['languageContactTag']
        styleAssignmentContactTag = request.POST['styleAssignmentContactTag']
        userIdFieldValue = request.POST['userIdFieldValue']
        compatabilityGuidesFieldValue = request.POST['compatabilityGuidesFieldValue']
        couplesGuidesFieldValue = request.POST['couplesGuidesFieldValue']
        product8DatesFieldValue = request.POST['product8DatesFieldValue']
        partnersFirstNameFieldValue = request.POST['partnersFirstNameFieldValue']
        partnersGenderFieldValue = request.POST['partnersGenderFieldValue']
        partnersUserIdFieldValue = request.POST['partnersUserIdFieldValue']
        partnerStyleAssignmentContactTag = request.POST['partnerStyleAssignmentContactTag']

        if is_8_dates == 'option1':
            is_8_dates = 'Yes'
        else:
            is_8_dates = 'No'

        try:
            obj = {
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'language': language,
                'gender': gender,
                'age': age,
                'style_assignment': style_assignment,
                'user_id': user_id,
                'compatability_guides': compatability_guides,
                'couples_guides': couples_guides,
                'product_8_dates': product_8_dates,
                'is_8_dates': is_8_dates,
                'partners_first_name': partners_first_name,
                'partners_gender': partners_gender,
                'partners_user_id': partners_user_id,
                'partner_style_assignment': partner_style_assignment,
                'contact_id':contact_id,
                'fieldValues': {
                    'ageFieldValue': ageFieldValue,
                    "genderFieldValue": genderFieldValue,
                    "languageContactTag": languageContactTag,
                    "styleAssignmentContactTag": styleAssignmentContactTag,
                    "userIdFieldValue": userIdFieldValue,
                    "compatabilityGuidesFieldValue": compatabilityGuidesFieldValue,
                    "couplesGuidesFieldValue": couplesGuidesFieldValue,
                    "product8DatesFieldValue": product8DatesFieldValue,
                    "partnersFirstNameFieldValue": partnersFirstNameFieldValue,
                    "partnersGenderFieldValue": partnersGenderFieldValue,
                    "partnersUserIdFieldValue": partnersUserIdFieldValue,
                    "partnerStyleAssignmentContactTag":partnerStyleAssignmentContactTag
                }
            }
            r = requests.post('https://x5lper6q2g.execute-api.us-east-1.amazonaws.com/v1/updatecontactintoactiveaampaign', json=obj)
            data = r.json()
            fieldValues = data['fieldValues']
            genderFieldValue = fieldValues['genderFieldValue']
            ageFieldValue = fieldValues['ageFieldValue']
            languageContactTag = fieldValues['languageContactTag']
            styleAssignmentContactTag = fieldValues['styleAssignmentContactTag']
            userIdFieldValue = fieldValues['userIdFieldValue']
            compatabilityGuidesFieldValue = fieldValues['compatabilityGuidesFieldValue']
            couplesGuidesFieldValue = fieldValues['couplesGuidesFieldValue']
            product8DatesFieldValue = fieldValues['product8DatesFieldValue']
            partnersFirstNameFieldValue = fieldValues['partnersFirstNameFieldValue']
            partnersGenderFieldValue = fieldValues['partnersGenderFieldValue']
            partnersUserIdFieldValue = fieldValues['partnersUserIdFieldValue']
            partnerStyleAssignmentContactTag = fieldValues['partnerStyleAssignmentContactTag']
            msg = data['message']
            messages.success(request, mark_safe(msg))
            return render(request, 'update_form.html', {
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'language': language,
                'gender': gender,
                'age': age,
                'style_assignment': style_assignment,
                'user_id': user_id,
                'compatability_guides': compatability_guides,
                'couples_guides': couples_guides,
                'product_8_dates': product_8_dates,
                'is_8_dates': is_8_dates,
                'partners_first_name': partners_first_name,
                'partners_gender': partners_gender,
                'partners_user_id': partners_user_id,
                'partner_style_assignment': partner_style_assignment,
                'contact_id': contact_id,
                "genderFieldValue": genderFieldValue,
                'ageFieldValue': ageFieldValue,
                "languageContactTag": languageContactTag,
                "styleAssignmentContactTag": styleAssignmentContactTag,
                "userIdFieldValue": userIdFieldValue,
                "compatabilityGuidesFieldValue": compatabilityGuidesFieldValue,
                "couplesGuidesFieldValue": couplesGuidesFieldValue,
                "product8DatesFieldValue": product8DatesFieldValue,
                "partnersFirstNameFieldValue": partnersFirstNameFieldValue,
                "partnersGenderFieldValue": partnersGenderFieldValue,
                "partnersUserIdFieldValue": partnersUserIdFieldValue,
                "partnerStyleAssignmentContactTag": partnerStyleAssignmentContactTag
            })

        except Exception as e:
            messages.error(request, "Error: {}".format(e))
            return render(request, 'update_form.html', {
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'language': language,
                'gender': gender,
                'age': age,
                'style_assignment': style_assignment,
                'user_id': user_id,
                'compatability_guides': compatability_guides,
                'couples_guides': couples_guides,
                'product_8_dates': product_8_dates,
                'is_8_dates': is_8_dates,
                'partners_first_name': partners_first_name,
                'partners_gender': partners_gender,
                'partners_user_id': partners_user_id,
                'partner_style_assignment': partner_style_assignment,
                'contact_id': contact_id,
                "genderFieldValue": genderFieldValue,
                'ageFieldValue':ageFieldValue,
                "languageContactTag": languageContactTag,
                "styleAssignmentContactTag": styleAssignmentContactTag,
                "userIdFieldValue": userIdFieldValue,
                "compatabilityGuidesFieldValue": compatabilityGuidesFieldValue,
                "couplesGuidesFieldValue": couplesGuidesFieldValue,
                "product8DatesFieldValue": product8DatesFieldValue,
                "partnersFirstNameFieldValue": partnersFirstNameFieldValue,
                "partnersGenderFieldValue": partnersGenderFieldValue,
                "partnersUserIdFieldValue": partnersUserIdFieldValue,
                "partnerStyleAssignmentContactTag": partnerStyleAssignmentContactTag
            })

