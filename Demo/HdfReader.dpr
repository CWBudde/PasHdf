program HdfReader;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  HdfFile in '..\Source\HdfFile.pas';

procedure ReadHdfFile(FileName: TFileName);
var
  HdfFile: THdfFile;
begin
  // check if file exists
  if not FileExists(FileName) then
    Exit;

  HdfFile := THdfFile.Create;
  try
    HdfFile.LoadFromFile(FileName);
  finally
    HdfFile.Free;
  end;
end;

begin
  try
    if ParamCount >= 1 then
      ReadHdfFile(ParamStr(1));
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.

