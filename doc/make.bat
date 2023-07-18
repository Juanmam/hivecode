@ECHO OFF

pushd %~dp0

REM Command file for Sphinx documentation

if "%SPHINXBUILD%" == "" (
    set SPHINXBUILD=sphinx-build
)
set SOURCEDIR=source
set BUILDDIR=build

%SPHINXBUILD% >NUL 2>NUL
if errorlevel 9009 (
    echo.
    echo.The 'sphinx-build' command was not found. Make sure you have Sphinx
    echo.installed, then set the SPHINXBUILD environment variable to point
    echo.to the full path of the 'sphinx-build' executable. Alternatively, you
    echo.may add the Sphinx directory to PATH.
    echo.
    echo.If you don't have Sphinx installed, grab it from
    echo.https://www.sphinx-doc.org/
    exit /b 1
)

if "%1" == "" goto help

if "%2" == "plantuml" (
    @REM echo Generating PlantUML diagrams...
    @REM set IMAGESDIR=%SOURCEDIR%\images
    @REM echo Images directory: %IMAGESDIR%
    @REM mkdir %IMAGESDIR% 2>NUL
    @REM for %%f in (%SOURCEDIR%\*.puml) do (
    @REM     echo Generating diagram for file: %%f
    @REM     java -jar plantuml.jar -tpng "%%f" -o %IMAGESDIR%
    @REM )
    @REM echo PlantUML diagrams generated.
    %SPHINXBUILD% -b html -D plantuml='plantuml' %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
) else (
    %SPHINXBUILD% -M %1 %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
)

REM Open the index file
start %BUILDDIR%\html\index.html

goto end

:help
%SPHINXBUILD% -M help %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%

:end
popd
