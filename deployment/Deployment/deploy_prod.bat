@echo off
echo %1 %2

IF EXIST %1 (
	echo "deleting %1"
	rmdir /s /q %1 
)

IF EXIST %1.zip (
	echo "deleting %1.zip"
	del %1.zip
)

mkdir %1

xcopy deployment_packages %1 /s
xcopy "../../apis/%1" %1 /s

IF %1 == ProfilesGenerateUserGuidesReport (
	xcopy ForGuidesReport %1 /s
)

IF %1 == ProfilesGenerateInterpersonalReport (
	xcopy ForInterpersonal %1 /s
)

IF %1 == ProfilesGenerateUserProfileReport (
	xcopy ForDevProfileReport %1 /s
)

IF %1 == ProfilesGetLanguage (
	copy additional\GeoLite2-Country.mmdb %1
)

IF %1 == ProfilesGenericProfileImage (
	xcopy ForProfileImages %1 /s
)

IF %1 == ProfilesGenericSimilarityImage (
	xcopy ForSimilarityImages %1 /s
)

powershell Compress-Archive %1/* %1.zip

aws s3 cp %1.zip s3://profiles-deployment-prod
aws lambda update-function-code --function-name %1%2 --s3-bucket profiles-deployment-prod --s3-key %1.zip

echo "deleting %1"
rmdir /s /q %1
	
echo "deleting %1.zip"
del %1.zip







