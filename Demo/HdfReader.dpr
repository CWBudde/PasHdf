program HdfReader;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  SysUtils, StrUtils,
  HdfFile in '..\Source\HdfFile.pas';

procedure PrintDataObjectInformation(DataObject: THdfDataObject; Indent: Integer);
var
  Index: Integer;
  IndentStr: string;
begin
  IndentStr := DupeString(' ', Indent);
  if Indent > 2 then
    WriteLn('');
  WriteLn(IndentStr + 'Name: ' + DataObject.Name);
  WriteLn(IndentStr + 'Data Class: ' + IntToStr(DataObject.DataType.DataClass));
  WriteLn(IndentStr + 'Data Dimensionality: ' + IntToStr(DataObject.DataSpace.Dimensionality));
  if DataObject.DataSpace.Dimensionality > 0 then
  begin
    for Index := 0 to DataObject.DataSpace.Dimensionality - 1 do
      WriteLn(IndentStr + '  ' + IntToStr(DataObject.DataSpace.Dimension[Index]));
  end;

  if DataObject.AttributeListCount > 0 then
  begin
    WriteLn(IndentStr + 'Attributes: ');
    for Index := 0 to DataObject.AttributeListCount - 1 do
      WriteLn(IndentStr + '  ' + DataObject.AttributeListItem[Index].Name + ': ' + DataObject.AttributeListItem[Index].ValueAsString);
  end;

(*
  if DataObject.DataLayoutChunkCount > 0 then
  begin
    WriteLn(IndentStr + 'Data Layout: ');
    for Index := 0 to DataObject.DataLayoutChunkCount - 1 do
      WriteLn(IndentStr + '  ' + IntToStr(DataObject.DataLayoutChunk[Index]));
  end;
*)

  // write data objects
  if DataObject.DataObjectCount > 0 then
  begin
    WriteLn(IndentStr + 'Data Objects: ');
    for Index := 0 to DataObject.DataObjectCount - 1 do
      PrintDataObjectInformation(DataObject.DataObject[Index], Indent + 2);
  end;
end;

procedure PrintFileInformation(HdfFile: THdfFile);
begin
  WriteLn('Super block:');
  WriteLn('  Offset size: ' + IntToStr(Integer(HdfFile.SuperBlock.OffsetSize)));
  WriteLn('  Lengths size: ' + IntToStr(Integer(HdfFile.SuperBlock.LengthsSize)));
  WriteLn('  End of file address: ' + IntToStr(HdfFile.SuperBlock.EndOfFileAddress));
  WriteLn('');
  WriteLn('Base Data Object:');
  PrintDataObjectInformation(HdfFile.DataObject, 2);
end;

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
    PrintFileInformation(HdfFile);
  finally
    HdfFile.Free;
  end;

  {$IFDEF DEBUG}
  ReadLn;
  {$ENDIF}
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

