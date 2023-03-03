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

IF %1 == ProfilesGetGuideReport (
    xcopy deployment_packages %1 /s
    xcopy "../../../algorithms/%1" %1 /s
	xcopy ForUserGuidesReport %1 /s
)

powershell Compress-Archive %1/* %1.zip

aws s3 cp %1.zip s3://profiles-deployment
aws lambda update-function-code --function-name %1%2 --s3-bucket profiles-deployment --s3-key %1.zip

echo "deleting %1"
rmdir /s /q %1
	
echo "deleting %1.zip"
del %1.zip







