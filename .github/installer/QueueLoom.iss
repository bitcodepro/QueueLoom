; Built from the same staging directory as the portable ZIP.
[Setup]
AppId=QueueLoom
AppName=QueueLoom
AppVersion={#ProductVersion}
AppPublisher=bitcodepro
AppPublisherURL=https://github.com/bitcodepro/QueueLoom
DefaultDirName={localappdata}\Programs\QueueLoom
UsePreviousAppDir=yes
DisableDirPage=yes
DisableProgramGroupPage=yes
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
UninstallDisplayIcon={app}\QueueLoom.exe
SetupIconFile=..\..\src\QueueLoom.App\Assets\queueloom.ico
OutputDir={#PackageOutput}
OutputBaseFilename=QueueLoom-{#ProductVersion}-win-x64-setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
UsePreviousTasks=yes

[Tasks]
Name: desktopicon; Description: "Create a desktop shortcut"; Flags: unchecked

[Files]
; Put temporary files first so solid extraction does not traverse the launcher.
Source: "{#PackageInput}\QueueLoom.bootstrap.json"; Flags: dontcopy noencryption
Source: "{#PackageInput}\QueueLoom.bootstrap.zip"; Flags: dontcopy noencryption
; A manual upgrade keeps the installed entry and fallback bootstrap byte-for-byte.
Source: "{#PackageInput}\QueueLoom.exe"; DestDir: "{app}"; Check: InstallBootstrap
Source: "{#PackageInput}\QueueLoom.bootstrap.json"; DestDir: "{app}"; Check: InstallBootstrap
Source: "{#PackageInput}\QueueLoom.bootstrap.zip"; DestDir: "{app}"; Check: InstallBootstrap
Source: "{#PackageInput}\README.md"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#PackageInput}\LICENSE"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#PackageInput}\THIRD-PARTY-NOTICES.md"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#PackageInput}\docs\stable-launcher.md"; DestDir: "{app}\docs"; Flags: ignoreversion

[Icons]
Name: "{userprograms}\QueueLoom"; Filename: "{app}\QueueLoom.exe"; WorkingDir: "{app}"
Name: "{userdesktop}\QueueLoom"; Filename: "{app}\QueueLoom.exe"; WorkingDir: "{app}"; Tasks: desktopicon

[Run]
Filename: "{app}\QueueLoom.exe"; Description: "Launch QueueLoom"; WorkingDir: "{app}"; Flags: postinstall nowait skipifsilent

[Code]
const
  InstallKey = 'Software\Microsoft\Windows\CurrentVersion\Uninstall\QueueLoom_is1';
  OwnerMarker = '\.queueloom-installer';

var
  Existing: Boolean;
  Reinstall: Boolean;

function InstallBootstrap: Boolean;
begin
  Result := not Existing;
end;

function DirectoryIsEmpty(Path: String): Boolean;
var
  Entry: TFindRec;
begin
  Result := True;
  if FindFirst(Path + '\*', Entry) then
  try
    repeat
      if (Entry.Name <> '.') and (Entry.Name <> '..') then
      begin
        Result := False;
        Exit;
      end;
    until not FindNext(Entry);
  finally
    FindClose(Entry);
  end;
end;

function StagePackage(AppDir, PackageDir: String): Boolean;
var
  ExitCode: Integer;
begin
  Result := Exec(AppDir + '\QueueLoom.exe',
    '--installer-stage-package "' + PackageDir + '"', AppDir, SW_HIDE,
    ewWaitUntilTerminated, ExitCode) and (ExitCode = 0);
end;

function PrepareToInstall(var NeedsRestart: Boolean): String;
var
  AppDir, RegisteredDir: String;
  Marker: AnsiString;
begin
  Result := '';
  AppDir := ExpandConstant('{app}');
  Existing := RegQueryStringValue(HKCU, InstallKey, 'InstallLocation', RegisteredDir);
  Reinstall := False;
  if Existing then
  begin
    if CompareText(RemoveBackslashUnlessRoot(RegisteredDir), AppDir) <> 0 then
    begin
      Result := 'Uninstall QueueLoom before changing the installation directory.';
      Exit;
    end;
    if not FileExists(AppDir + '\QueueLoom.exe') then
    begin
      Result := 'The installed QueueLoom launcher is missing. Uninstall QueueLoom before reinstalling it.';
      Exit;
    end;
    ExtractTemporaryFile('QueueLoom.bootstrap.json');
    ExtractTemporaryFile('QueueLoom.bootstrap.zip');
    if not StagePackage(AppDir, ExpandConstant('{tmp}')) then
      Result := 'QueueLoom could not stage this version. Close QueueLoom and its MCP clients, finish any pending update by launching the app, and retry with the latest installer.';
  end
  else if DirExists(AppDir) and not DirectoryIsEmpty(AppDir) then
  begin
    Reinstall := LoadStringFromFile(AppDir + OwnerMarker, Marker) and
      (Marker = 'QueueLoom Installer v1') and
      not FileExists(AppDir + '\QueueLoom.exe') and
      not FileExists(AppDir + '\QueueLoom.bootstrap.json') and
      not FileExists(AppDir + '\QueueLoom.bootstrap.zip');
    if not Reinstall then
      Result := 'The installation directory contains existing files. Move that portable installation to a different folder before running Setup.';
  end;
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  AppDir: String;
begin
  if CurStep = ssPostInstall then
  begin
    AppDir := ExpandConstant('{app}');
    if Reinstall and DirExists(AppDir + '\QueueLoom.versions') then
      if not StagePackage(AppDir, AppDir) then
        SuppressibleMsgBox('Existing QueueLoom update state was retained. Finish any pending update by launching QueueLoom, then use the app updater or run the latest installer again.', mbInformation, MB_OK, IDOK);
    // Deliberately retain this marker with adjacent backups and prior payloads on uninstall.
    if not SaveStringToFile(AppDir + OwnerMarker, 'QueueLoom Installer v1', False) then
      Log('QueueLoom could not record the installer ownership marker.');
  end;
end;
