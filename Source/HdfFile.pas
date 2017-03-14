unit HdfFile;

interface

{-$DEFINE IgnoreWrongPosition}

uses
  Classes, SysUtils, Contnrs;

type
  EHdfInvalidFormat = class(Exception);

  TStreamHelper = class helper for TStream
    procedure ReadExcept(var Buffer; Count: Longint; ExceptionMessage: string);
  end;

  THdfSignature = array [0..3] of AnsiChar;

  THdfSuperBlock = class(TInterfacedPersistent, IStreamPersist)
  private
    FFormatSignature: array [0..7] of AnsiChar;
    FVersion: Byte;
    FConsistencyFlag: Byte;
    FOffsetSize: Byte;
    FLengthsSize: Byte;
    FBaseAddress: Int64;
    FSuperBlockExtensionAddress: Int64;
    FEndOfFileAddress: Int64;
    FRootGroupObjectHeaderAddress: Int64;
    FChecksum: Integer;
  public
    procedure LoadFromStream(Stream: TStream);
    procedure SaveToStream(Stream: TStream);

    property OffsetSize: Byte read FOffsetSize;
    property LengthsSize: Byte read FLengthsSize;
    property EndOfFileAddress: Int64 read FEndOfFileAddress;
  end;

  THdfDataObject = class;

  THdfDataObjectMessage = class(TInterfacedPersistent, IStreamPersist)
  private
    FSuperBlock: THdfSuperBlock;
    FDataObject: THdfDataObject;
  protected
    FVersion: Byte;
    property Superblock: THdfSuperBlock read FSuperBlock;
    property DataObject: THdfDataObject read FDataObject;
  public
    constructor Create(SuperBlock: THdfSuperBlock; DataObject: THdfDataObject);

    procedure LoadFromStream(Stream: TStream); virtual;
    procedure SaveToStream(Stream: TStream);

    property Version: Byte read FVersion;
  end;

  THdfMessageDataSpace = class(THdfDataObjectMessage)
  private
    FDimensionality: Byte;
    FFlags: Byte;
    FType: Byte;
    FDimensionSize: array of Int64;
    FDimensionMaxSize: array of Int64;
    function GetDimension(Index: Integer): Integer;
  public
    procedure LoadFromStream(Stream: TStream); override;

    property Dimensionality: Byte read FDimensionality;
    property Dimension[Index: Integer]: Integer read GetDimension;
  end;

  THdfMessageLinkInfo = class(THdfDataObjectMessage)
  private
    FFlags: Byte;
    FMaximumCreationIndex: Int64;
    FFractalHeapAddress: Int64;
    FAddressBTreeIndex: Int64;
    FAddressBTreeOrder: Int64;
  public
    procedure LoadFromStream(Stream: TStream); override;

    property FractalHeapAddress: Int64 read FFractalHeapAddress;
  end;

  THdfMessageDataType = class;

  THdfBaseDataType = class(TInterfacedPersistent, IStreamPersist)
  private
    FDataTypeMessage: THdfMessageDataType;
  public
    constructor Create(DatatypeMessage: THdfMessageDataType); virtual;

    procedure LoadFromStream(Stream: TStream); virtual;
    procedure SaveToStream(Stream: TStream);
  end;

  THdfDataTypeFixedPoint = class(THdfBaseDataType)
  private
    FBitOffset: Word;
    FBitPrecision: Word;
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfDataTypeFloatingPoint = class(THdfBaseDataType)
  private
    FBitOffset: Word;
    FBitPrecision: Word;
    FExponentLocation: Byte;
    FExponentSize: Byte;
    FMantissaLocation: Byte;
    FMantissaSize: Byte;
    FExponentBias: Integer;
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfDataTypeTime = class(THdfBaseDataType)
  private
    FBitPrecision: Word;
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfDataTypeString = class(THdfBaseDataType);

  THdfDataTypeBitfield = class(THdfBaseDataType)
  private
    FBitOffset: Word;
    FBitPrecision: Word;
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfDataTypeOpaque = class(THdfBaseDataType);

  THdfDataTypeCompoundPart = class
  private
    FName: UTF8String;
    FByteOffset: Int64;
    FSize: Int64;
    FDataType: THdfMessageDataType;
  public
    constructor Create(DatatypeMessage: THdfMessageDataType); virtual;

    procedure ReadFromStream(Stream: TStream);
  end;

  THdfDataTypeCompound = class(THdfBaseDataType)
  private
    FDataTypes: TObjectList;
  public
    constructor Create(DatatypeMessage: THdfMessageDataType); virtual;
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfDataTypeReference = class(THdfBaseDataType);
  THdfDataTypeEnumerated = class(THdfBaseDataType);
  THdfDataTypeVariableLength = class(THdfBaseDataType)
  private
    FDataType: THdfMessageDataType;
  public
    constructor Create(DatatypeMessage: THdfMessageDataType); override;
    procedure LoadFromStream(Stream: TStream); override;
  end;
  THdfDataTypeArray = class(THdfBaseDataType);

  THdfMessageDataType = class(THdfDataObjectMessage)
  private
    FDataClass: Byte;
    FClassBitField: array [0..2] of Byte;
    FSize: Integer;
    FDataType: THdfBaseDataType;
  public
    procedure LoadFromStream(Stream: TStream); override;

    property Size: Integer read FSize;
    property DataClass: Byte read FDataClass;
  end;

  THdfMessageDataFill = class(THdfDataObjectMessage)
  private
    FFlags: Byte;
    FSize: Integer;
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfMessageDataLayout = class(THdfDataObjectMessage)
  private
    FLayoutClass: Byte;
    FDataAddress: Int64;
    FDataSize: Int64;
    FDimensionality: Byte;
    procedure ReadTree(Stream: TStream; Size: Integer);
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfMessageGroupInfo = class(THdfDataObjectMessage)
  private
    FFlags: Byte;
    FMaximumCompact: Word;
    FMinimumDense: Word;
    FEstimatedNumberOfEntries: Word;
    FEstimatedLinkNameLength: Word;
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfMessageFilterPipeline = class(THdfDataObjectMessage)
  private
    FFilters: Byte;
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfAttribute = class
  private
    FName: UTF8String;
    FStream: TMemoryStream;
    function GetValueAsString: UTF8String;
    procedure SetValueAsString(const Value: UTF8String);
  public
    constructor Create(Name: UTF8String);

    property Name: UTF8String read FName;
    property ValueAsString: UTF8String read GetValueAsString write SetValueAsString;
  end;

  THdfMessageAttribute = class(THdfDataObjectMessage)
  private
    FFlags: Byte;
    FNameSize: Word;
    FDatatypeSize: Word;
    FDataspaceSize: Word;
    FEncoding: Byte;
    FName: UTF8String;
    FDatatypeMessage: THdfMessageDataType;
    FDataspaceMessage: THdfMessageDataSpace;
    procedure ReadData(Stream: TStream; Attribute: THdfAttribute);
    procedure ReadDataDimension(Stream: TStream; Attribute: THdfAttribute;
      Dimension: Integer);
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfMessageHeaderContinuation = class(THdfDataObjectMessage)
  private
    FOffset: Int64;
    FLength: Int64;
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfMessageAttributeInfo = class(THdfDataObjectMessage)
  private
    FFlags: Byte;
    FMaximumCreationIndex: Word;
    FFractalHeapAddress: Int64;
    FAttributeNameBTreeAddress: Int64;
    FAttributeOrderBTreeAddress: Int64;
  public
    procedure LoadFromStream(Stream: TStream); override;

    property FractalHeapAddress: Int64 read FFractalHeapAddress;
  end;

  THdfFractalHeap = class(TInterfacedPersistent, IStreamPersist)
  private
    FSuperBlock: THdfSuperBlock;
    FDataObject: THdfDataObject;
    FSignature: THdfSignature;
    FVersion: Byte;
    FHeapIdLength: Word;
    FEncodedLength: Word;
    FFlags: Byte;
    FMaximumSize: Integer;
    FNextHugeID: Int64;
    FBtreeAddresses: Int64;
    FAmountFreeSpace: Int64;
    FAddressManagedBlock: Int64;
    FAmountManagedSpace: Int64;
    FAmountAllocatedManagedSpace: Int64;
    FOffsetDirectBlockAllocation: Int64;
    FNumberOfManagedObjects: Int64;
    FSizeOfHugeObjects: Int64;
    FNumberOfHugeObjects: Int64;
    FSizeOfTinyObjects: Int64;
    FNumberOfTinyObjects: Int64;
    FTableWidth: Word;
    FStartingBlockSize: Int64;
    FMaximumDirectBlockSize: Int64;
    FMaximumHeapSize: Word;
    FStartingNumber: Word;
    FAddressOfRootBlock: Int64;
    FCurrentNumberOfRows: Word;
    FSizeOfFilteredRootDirectBlock: Int64;
    FIOFilterMask: Integer;
    FIOFilterInformation: Int64;
    FChecksum: Integer;
  protected
    property SuperBlock: THdfSuperBlock read FSuperBlock;
  public
    constructor Create(SuperBlock: THdfSuperBlock; DataObject: THdfDataObject);

    procedure LoadFromStream(Stream: TStream);
    procedure SaveToStream(Stream: TStream);

    property MaximumSize: Integer read FMaximumSize;
    property MaximumDirectBlockSize: Int64 read FMaximumDirectBlockSize;
    property MaximumHeapSize: Word read FMaximumHeapSize;
    property StartingBlockSize: Int64 read FStartingBlockSize;
    property Flags: Byte read FFlags;
    property TableWidth: Word read FTableWidth;
    property EncodedLength: Word read FEncodedLength;
  end;

  THdfCustomBlock = class(TInterfacedPersistent, IStreamPersist)
  private
    FSuperBlock: THdfSuperBlock;
    FFractalHeap: THdfFractalHeap;
    FDataObject: THdfDataObject;
    FSignature: THdfSignature;
    FVersion: Byte;
    FHeapHeaderAddress: Int64;
    FBlockOffset: Int64;
    FChecksum: Integer;
  protected
    class function GetSignature: THdfSignature; virtual; abstract;

    property SuperBlock: THdfSuperBlock read FSuperBlock;
  public
    constructor Create(SuperBlock: THdfSuperBlock;
      FractalHeap: THdfFractalHeap; DataObject: THdfDataObject); virtual;

    procedure LoadFromStream(Stream: TStream); virtual;
    procedure SaveToStream(Stream: TStream);
  end;

  THdfDirectBlock = class(THdfCustomBlock)
  protected
    class function GetSignature: THdfSignature; override;
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfIndirectBlock = class(THdfCustomBlock)
  private
    FInitialBlockSize: Int64;
    FMaximumNumberOfDirectBlockRows: Integer;
  protected
    class function GetSignature: THdfSignature; override;
  public
    constructor Create(SuperBlock: THdfSuperBlock; FractalHeap: THdfFractalHeap;
      DataObject: THdfDataObject); override;

    procedure LoadFromStream(Stream: TStream); override;
  end;

  TArrayOfInteger = array of Integer;

  THdfDataObject = class(TInterfacedPersistent, IStreamPersist)
  private
    FName: UTF8String;
    FSuperBlock: THdfSuperBlock;
    FSignature: THdfSignature;
    FVersion: Byte;
    FFlags: Byte;
    FAccessTime: Integer;
    FModificationTime: Integer;
    FChangeTime: Integer;
    FBirthTime: Integer;
    FChunkSize: Int64;
    FMaximumCompact: Word;
    FMinimumDense: Word;

    FDataLayoutChunk: TArrayOfInteger;

    FData: TMemoryStream;
    FAttributeList: TObjectList;

    FDataType: THdfMessageDataType;
    FDataSpace: THdfMessageDataSpace;
    FLinkInfo: THdfMessageLinkInfo;
    FGroupInfo: THdfMessageGroupInfo;
    FAttributeInfo: THdfMessageAttributeInfo;

    FDataObjects: TObjectList;

    FAttributesHeap: THdfFractalHeap;
    FObjectsHeap: THdfFractalHeap;
    function GetDataObjectCount: Integer;
    function GetDataObject(Index: Integer): THdfDataObject;
    function GetDataLayoutChunk(Index: Integer): Integer;
    function GetDataLayoutChunkCount: Integer;
    function GetAttributeListCount: Integer;
    function GetAttributeListItem(Index: Integer): THdfAttribute;
  protected
    procedure ReadObjectHeaderMessages(Stream: TStream; EndOfStream: Int64);

    property Superblock: THdfSuperBlock read FSuperBlock;
    property AttributesHeap: THdfFractalHeap read FAttributesHeap;
    property ObjectsHeap: THdfFractalHeap read FObjectsHeap;
  public
    constructor Create(SuperBlock: THdfSuperBlock); overload;
    constructor Create(SuperBlock: THdfSuperBlock; Name: UTF8String); overload;

    procedure AddDataObject(DataObject: THdfDataObject);
    procedure AddAttribute(Attribute: THdfAttribute);

    procedure LoadFromStream(Stream: TStream);
    procedure SaveToStream(Stream: TStream);

    property Name: UTF8String read FName;
    property Data: TMemoryStream read FData;
    property DataType: THdfMessageDataType read FDataType;
    property DataSpace: THdfMessageDataSpace read FDataSpace;
    property LinkInfo: THdfMessageLinkInfo read FLinkInfo;
    property GroupInfo: THdfMessageGroupInfo read FGroupInfo;
    property AttributeInfo: THdfMessageAttributeInfo read FAttributeInfo;

    property AttributeListCount: Integer read GetAttributeListCount;
    property AttributeListItem[Index: Integer]: THdfAttribute read GetAttributeListItem;
    property DataObjectCount: Integer read GetDataObjectCount;
    property DataObject[Index: Integer]: THdfDataObject read GetDataObject;
    property DataLayoutChunkCount: Integer read GetDataLayoutChunkCount;
    property DataLayoutChunk[Index: Integer]: Integer read GetDataLayoutChunk;
  end;

  THdfFile = class(TInterfacedPersistent, IStreamPersist)
  private
    FSuperBlock: THdfSuperBlock;
    FDataObject: THdfDataObject;
  public
    procedure AfterConstruction; override;

    procedure LoadFromStream(Stream: TStream);
    procedure SaveToStream(Stream: TStream);

    procedure LoadFromFile(Filename: TFileName);
    procedure SaveToFile(Filename: TFileName);

    property SuperBlock: THdfSuperBlock read FSuperBlock;
    property DataObject: THdfDataObject read FDataObject;
  end;

implementation

uses
  Math;

{ TStreamHelper }

procedure TStreamHelper.ReadExcept(var Buffer; Count: Integer;
  ExceptionMessage: string);
begin
  if Read(Buffer, Count) <> Count then
    raise Exception.Create(ExceptionMessage);
end;


{ THdfSuperBlock }

procedure THdfSuperBlock.LoadFromStream(Stream: TStream);
begin
  Stream.ReadExcept(FFormatSignature[0], 8, 'Error reading signature');
  if FFormatSignature <> '‰HDF'#$D#$A#$1A#$A then
    raise Exception.Create('The file is not a valid HDF');

  // read version
  Stream.ReadExcept(FVersion, 1, 'Error reading version');
  if not (FVersion in [2, 3]) then
    raise Exception.Create('Unsupported version');

  // read offset & length size
  Stream.ReadExcept(FOffsetSize, 1, 'Error reading offset size');
  Stream.ReadExcept(FLengthsSize, 1, 'Error reading lengths size');

  // read consistency flag
  Stream.ReadExcept(FConsistencyFlag, 1, 'Error reading consistency flag');

  // read base address
  Stream.ReadExcept(FBaseAddress, FOffsetSize, 'Error reading base address');

  // read superblock extension address
  Stream.ReadExcept(FSuperBlockExtensionAddress, FOffsetSize, 'Error reading superblock extension address');

  // read end of file address
  Stream.ReadExcept(FEndOfFileAddress, FOffsetSize, 'Error reading end of file address');

  // read group object header address
  Stream.ReadExcept(FRootGroupObjectHeaderAddress, FOffsetSize, 'Error reading group object header address');

  if FBaseAddress <> 0 then
    raise Exception.Create('The base address should be zero');
  if FEndOfFileAddress <> Stream.Size then
    raise Exception.Create('Size mismatch');

  // read checksum
  Stream.ReadExcept(FChecksum, 4, 'Error reading checksum');

  // read checksum
  if Stream.Seek(FRootGroupObjectHeaderAddress, soFromBeginning) <> FRootGroupObjectHeaderAddress then
    raise Exception.Create('Error seeking first object');
end;

procedure THdfSuperBlock.SaveToStream(Stream: TStream);
begin
  raise Exception.Create('Not yet implemented');
end;


{ THdfDataObjectMessage }

constructor THdfDataObjectMessage.Create(SuperBlock: THdfSuperBlock; DataObject: THdfDataObject);
begin
  FSuperBlock := SuperBlock;
  FDataObject := DataObject;
end;

procedure THdfDataObjectMessage.LoadFromStream(Stream: TStream);
begin
  // read version
  Stream.ReadExcept(FVersion, 1, 'Error reading version');
end;

procedure THdfDataObjectMessage.SaveToStream(Stream: TStream);
begin
  raise Exception.Create('Not yet implemented');
end;


{ THdfMessageDataSpace }

procedure THdfMessageDataSpace.LoadFromStream(Stream: TStream);
var
  Index: Integer;
begin
  inherited;

  if not (FVersion in [1, 2]) then
    raise Exception.Create('Unsupported version of dataspace message');

  // read dimensionality
  Stream.ReadExcept(FDimensionality, 1, 'Error reading dimensionality');

  // read flags
  Stream.ReadExcept(FFlags, 1, 'Error reading flags');

  // eventually skip reserved
  if FVersion = 1 then
  begin
    Stream.Seek(5, soFromCurrent);

    raise Exception.Create('Unsupported version of dataspace message');
  end;

  // read type
  Stream.ReadExcept(FType, 1, 'Error reading type');

  // read dimension size
  SetLength(FDimensionSize, FDimensionality);
  for Index := 0 to FDimensionality - 1 do
    Stream.ReadExcept(FDimensionSize[Index], Superblock.LengthsSize, 'Error reading dimension size');

  // eventually read dimension max size
  if (FFlags and 1) <> 0 then
  begin
    SetLength(FDimensionMaxSize, FDimensionality);
    for Index := 0 to FDimensionality - 1 do
      Stream.ReadExcept(FDimensionMaxSize[Index], Superblock.LengthsSize, 'Error reading dimension size');
  end;
end;

function THdfMessageDataSpace.GetDimension(Index: Integer): Integer;
begin
  if (Index < 0) or (Index >= Length(FDimensionSize)) then
    raise Exception.CreateFmt('Index out of bounds (%d)', [Index]);

  Result := FDimensionSize[Index];
end;


{ THdfBaseDataType }

constructor THdfBaseDataType.Create(DatatypeMessage: THdfMessageDataType);
begin
  FDataTypeMessage := DataTypeMessage;
end;

procedure THdfBaseDataType.LoadFromStream(Stream: TStream);
begin
  // do nothing by default
end;

procedure THdfBaseDataType.SaveToStream(Stream: TStream);
begin
  raise Exception.Create('Not yet implemented');
end;


{ THdfDataTypeFixedPoint }

procedure THdfDataTypeFixedPoint.LoadFromStream(Stream: TStream);
begin
  inherited;

  Stream.ReadExcept(FBitOffset, 2, 'Error reading bit offset');
  Stream.ReadExcept(FBitPrecision, 2, 'Error reading bit precision');
end;


{ THdfDataTypeFloatingPoint }

procedure THdfDataTypeFloatingPoint.LoadFromStream(Stream: TStream);
begin
  Stream.ReadExcept(FBitOffset, 2, 'Error reading bit offset');
  Stream.ReadExcept(FBitPrecision, 2, 'Error reading bit precision');

  Stream.ReadExcept(FExponentLocation, 1, 'Error reading exponent location');
  Stream.ReadExcept(FExponentSize, 1, 'Error reading exponent size');
  Stream.ReadExcept(FMantissaLocation, 1, 'Error reading mantissa location');
  Stream.ReadExcept(FMantissaSize, 1, 'Error reading mantissa size');
  Stream.ReadExcept(FExponentBias, 4, 'Error reading exponent bias');

  if (FBitOffset <> 0) then
    raise Exception.Create('Unsupported bit offset');
  if (FMantissaLocation <> 0) then
    raise Exception.Create('Unsupported mantissa location');
  if (FBitPrecision = 32) then
  begin
    if (FExponentLocation <> 23) then
      raise Exception.Create('Unsupported exponent location');
    if (FExponentSize <> 8) then
      raise Exception.Create('Unsupported exponent size');
    if (FMantissaSize <> 23) then
      raise Exception.Create('Unsupported mantissa size');
    if (FExponentBias <> 127) then
      raise Exception.Create('Unsupported exponent bias');
  end else
  if (FBitPrecision = 64) then
  begin
    if (FExponentLocation <> 52) then
      raise Exception.Create('Unsupported exponent location');
    if (FExponentSize <> 11) then
      raise Exception.Create('Unsupported exponent size');
    if (FMantissaSize <> 52) then
      raise Exception.Create('Unsupported mantissa size');
    if (FExponentBias <> 1023) then
      raise Exception.Create('Unsupported exponent bias');
  end
  else
    raise Exception.Create('Unsupported bit precision');
end;


{ THdfDataTypeTime }

procedure THdfDataTypeTime.LoadFromStream(Stream: TStream);
begin
  Stream.ReadExcept(FBitPrecision, 2, 'Error reading bit precision');
end;


{ THdfDataTypeBitfield }

procedure THdfDataTypeBitfield.LoadFromStream(Stream: TStream);
begin
  Stream.ReadExcept(FBitOffset, 2, 'Error reading bit offset');
  Stream.ReadExcept(FBitPrecision, 2, 'Error reading bit precision');
end;


{ THdfDataTypeCompoundPart }

constructor THdfDataTypeCompoundPart.Create(DatatypeMessage: THdfMessageDataType);
begin
  FDataType := THdfMessageDataType.Create(DatatypeMessage.Superblock, DatatypeMessage.DataObject);
  FSize := DatatypeMessage.Size;
end;

procedure THdfDataTypeCompoundPart.ReadFromStream(Stream: TStream);
var
  Char: AnsiChar;
  ByteIndex: Integer;
  Temp: Byte;
begin
  FName := '';
  repeat
    Stream.ReadExcept(Char, 1, 'Error reading char');
    FName := FName + Char;
  until Char = #0;

  ByteIndex := 0;
  repeat
    Stream.ReadExcept(Temp, 1, 'Error reading value');
    FByteOffset := FByteOffset + Temp shl (8 * ByteIndex);
    Inc(ByteIndex);
  until 1 shl (8 * ByteIndex) > FSize;

  FDataType.LoadFromStream(Stream);
end;

{ THdfDataTypeCompound }

constructor THdfDataTypeCompound.Create(DatatypeMessage: THdfMessageDataType);
begin
  inherited Create(DatatypeMessage);

  FDataTypes := TObjectList.Create;
end;

procedure THdfDataTypeCompound.LoadFromStream(Stream: TStream);
var
  Index: Integer;
  Count: Integer;
  Part: THdfDataTypeCompoundPart;
begin
  if (FDataTypeMessage.Version <> 3) then
    raise Exception.CreateFmt('Error unsupported compound version (%d)', [FDataTypeMessage.Version]);

  Count := FDataTypeMessage.FClassBitField[1] shl 8 + FDataTypeMessage.FClassBitField[0];
  for Index := 0 to Count - 1 do
  begin
    Part := THdfDataTypeCompoundPart.Create(FDataTypeMessage);
    Part.ReadFromStream(Stream);
    FDataTypes.Add(Part);
  end;
end;


{ THdfDataTypeVariableLength }

constructor THdfDataTypeVariableLength.Create(
  DatatypeMessage: THdfMessageDataType);
begin
  inherited;

  FDataType := THdfMessageDataType.Create(FDataTypeMessage.Superblock, FDataTypeMessage.DataObject);
end;

procedure THdfDataTypeVariableLength.LoadFromStream(Stream: TStream);
begin
  FDataType.LoadFromStream(Stream);
end;


{ THdfMessageDataType }

procedure THdfMessageDataType.LoadFromStream(Stream: TStream);
begin
  inherited;

  // expand class and version
  FDataClass := FVersion and $F;
  FVersion := FVersion shr 4;

  // check version
  if not (FVersion in [1, 3]) then
    raise Exception.Create('Unsupported version of data type message');

  Stream.ReadExcept(FClassBitField[0], 3, 'Error reading class bit field');

  Stream.ReadExcept(FSize, 4, 'Error reading size');

  case FDataClass of
    0:
      FDataType := THdfDataTypeFixedPoint.Create(Self);
    1:
      FDataType := THdfDataTypeFloatingPoint.Create(Self);
    2:
      FDataType := THdfDataTypeTime.Create(Self);
    3:
      FDataType := THdfDataTypeString.Create(Self);
    4:
      FDataType := THdfDataTypeBitfield.Create(Self);
    5:
      FDataType := THdfDataTypeOpaque.Create(Self);
    6:
      FDataType := THdfDataTypeCompound.Create(Self);
    7:
      FDataType := THdfDataTypeReference.Create(Self);
    8:
      FDataType := THdfDataTypeEnumerated.Create(Self);
    9:
      FDataType := THdfDataTypeVariableLength.Create(Self);
    10:
      FDataType := THdfDataTypeArray.Create(Self);
    else
      raise Exception.CreateFmt('Unknown datatype (%d)', [FDataClass]);
  end;

  if Assigned(FDataType) then
    FDataType.LoadFromStream(Stream);
end;


{ THdfMessageDataFill }

procedure THdfMessageDataFill.LoadFromStream(Stream: TStream);
begin
  inherited;

  // check version
  if FVersion <> 3 then
    raise Exception.Create('Unsupported version of data fill message');

  // read flags
  Stream.ReadExcept(FFlags, 1, 'Error reading flags');

  if (FFlags and (1 shl 5)) <> 0 then
  begin
    Stream.ReadExcept(FSize, 4, 'Error reading size');
    Stream.Seek(FSize, soCurrent);
  end;
end;


{ THdfMessageDataLayout }

procedure THdfMessageDataLayout.LoadFromStream(Stream: TStream);
var
  Index: Integer;
  Temp: Integer;
  StreamPos: Int64;
  Size: Int64;
begin
  inherited;

  // check version
  if FVersion <> 3 then
    raise Exception.Create('Unsupported version of data layout message');

  Stream.ReadExcept(FLayoutClass, 1, 'Error reading layout class');
  case FLayoutClass of
    0: // compact storage
      begin
        // read data size
        Stream.ReadExcept(FDataSize, 2, 'Error reading data size');

        // read raw data
        DataObject.Data.CopyFrom(Stream, FDataSize);
      end;
    1: // continous storage
      begin
        // compact storage
        Stream.ReadExcept(FDataAddress, Superblock.OffsetSize, 'Error reading data address');
        Stream.ReadExcept(FDataSize, Superblock.LengthsSize, 'Error reading data lengths');

        if FDataAddress > 0 then
        begin
          StreamPos := Stream.Position;
          Stream.Position := FDataAddress;

          DataObject.Data.CopyFrom(Stream, FDataSize);

          Stream.Position := StreamPos;
        end;
      end;
    2:
      begin
        Stream.ReadExcept(FDimensionality, 1, 'Error reading dimensionality');
        Stream.ReadExcept(FDataAddress, Superblock.OffsetSize, 'Error reading data address');
        SetLength(FDataObject.FDataLayoutChunk, FDimensionality);
        for Index := 0 to FDimensionality - 1 do
          Stream.ReadExcept(FDataObject.FDataLayoutChunk[Index], 4, 'Error reading data layout chunk');

        Size := DataObject.FDataLayoutChunk[FDimensionality - 1];
        for Index := 0 to DataObject.DataSpace.Dimensionality - 1 do
          Size := Size * DataObject.DataSpace.FDimensionSize[Index];

        if (FDataAddress > 0) and (FDataAddress < Superblock.EndOfFileAddress) then
        begin
          StreamPos := Stream.Position;
          Stream.Position := FDataAddress;

          ReadTree(Stream, Size);

          Stream.Position := StreamPos;
        end;
      end;
  end;
end;

procedure THdfMessageDataLayout.ReadTree(Stream: TStream; Size: Integer);
var
  Signature: THdfSignature;
  NodeType, NodeLevel: Byte;
  EntriesUsed: Word;
  CheckSum: Integer;
  Key, AddressLeftSibling, AddressRightSibling: Int64;
  ElementIndex, DimensionIndex, Elements: Integer;
  ChunkSize, FilterMask: Cardinal;
  Start: array of Cardinal;
  BreakCondition: Cardinal;
  ChildPointer, StreamPos: Int64;

  i, j, err, olen: Integer;
  x, y, z, b, e, dy, dz, sx, sy, sz, dzy, szy: Integer;
  input, output: Pointer;
  //start[4],

  buf: array [0..3] of Byte;
begin
  if DataObject.DataSpace.Dimensionality > 3 then
    raise EHdfInvalidFormat.Create('Error reading dimensions');

  // read signature
  Stream.ReadExcept(Signature[0], 4, 'Error reading signature');
  if Signature <> 'TREE' then
    raise Exception.CreateFmt('Wrong signature (%s)', [string(Signature)]);

  Stream.ReadExcept(NodeType, 1, 'Error reading node type');
  Stream.ReadExcept(NodeLevel, 1, 'Error reading node level');

  Stream.ReadExcept(EntriesUsed, 2, 'Error reading entries used');
  Stream.ReadExcept(AddressLeftSibling, Superblock.OffsetSize, 'Error reading left sibling address');
  Stream.ReadExcept(AddressRightSibling, Superblock.OffsetSize, 'Error reading right sibling address');

  Elements := 1;
  for DimensionIndex := 0 to FDataObject.DataSpace.Dimensionality - 1 do
    Elements := Elements * FDataObject.DatalayoutChunk[DimensionIndex];

(*
  dy := FDataObject.DatalayoutChunk[1];
  dz := FDataObject.DatalayoutChunk[2];
  sx := FDataObject.DataSpace.Dimension[0];
  sy := FDataObject.DataSpace.Dimension[1];
  sz := FDataObject.DataSpace.Dimension[2];
  dzy := dz * dy;
  szy := sz * sy;
  Size := FDataObject.DatalayoutChunk[FDataObject.DataSpace.Dimensionality];
*)

  for ElementIndex := 0 to 2 * EntriesUsed - 1 do
  begin
    if NodeType = 0 then
      Stream.ReadExcept(Key, Superblock.LengthsSize, 'Error reading keys')
    else
    begin
      Stream.ReadExcept(ChunkSize, 4, 'Error reading chunk size');
      Stream.ReadExcept(FilterMask, 4, 'Error reading filter mask');
      if FilterMask <> 0 then
        raise Exception.Create('All filters must be enabled');

      SetLength(Start, DataObject.DataSpace.Dimensionality);
      for DimensionIndex := 0 to DataObject.DataSpace.Dimensionality - 1 do
        Stream.ReadExcept(Start[DimensionIndex], 8, 'Error reading start');

      Stream.ReadExcept(BreakCondition, 8, 'Error reading break condition');
      if BreakCondition <> 0 then
        Break;

      Stream.ReadExcept(ChildPointer, Superblock.OffsetSize, 'Error reading child pointer');

      // read data
      StreamPos := Stream.Position;
      Stream.Position := ChildPointer;

      Stream.Position := StreamPos;
    end;
  end;

  Stream.ReadExcept(CheckSum, 4, 'Error reading checksum');
end;


{ THdfMessageLinkInfo }

procedure THdfMessageLinkInfo.LoadFromStream(Stream: TStream);
begin
  inherited;

  // check version
  if FVersion <> 0 then
    raise Exception.Create('Unsupported version of link info message');

  // read flags
  Stream.ReadExcept(FFlags, 1, 'Error reading flags');

  if (FFlags and 1) <> 0 then
    Stream.ReadExcept(FMaximumCreationIndex, 8, 'Error reading maximum creation index');

  Stream.ReadExcept(FFractalHeapAddress, SuperBlock.OffsetSize,
    'Error reading maximum creation index');
  Stream.ReadExcept(FAddressBTreeIndex, SuperBlock.OffsetSize,
    'Error reading maximum creation index');

  if (FFlags and 2) <> 0 then
    Stream.ReadExcept(FAddressBTreeOrder, SuperBlock.OffsetSize,
      'Error reading maximum creation index');
end;


{ THdfMessageGroupInfo }

procedure THdfMessageGroupInfo.LoadFromStream(Stream: TStream);
begin
  inherited;

  // check version
  if FVersion <> 0 then
    raise Exception.Create('Unsupported version of group info message');

  // read flags
  Stream.ReadExcept(FFlags, 1, 'Error reading flags');

  if (FFlags and 1) <> 0 then
  begin
    Stream.ReadExcept(FMaximumCompact, 2, 'Error reading maximum compact value');
    Stream.ReadExcept(FMinimumDense, 2, 'Error reading maximum compact value');
  end;

  if (FFlags and 2) <> 0 then
  begin
    Stream.ReadExcept(FEstimatedNumberOfEntries, 2, 'Error reading estimated number of entries');
    Stream.ReadExcept(FEstimatedLinkNameLength, 2, 'Error reading estimated link name length of entries');
  end;
end;


{ THdfMessageFilterPipeline }

procedure THdfMessageFilterPipeline.LoadFromStream(Stream: TStream);
var
  Index: Integer;
  FilterIdentificationValue: Word;
  Flags, NumberClientDataValues: Word;
  ValueIndex: Integer;
  ClientData: Integer;
begin
  inherited;

  // check version
  if FVersion <> 2 then
    raise Exception.Create('Unsupported version of the filter pipeline message');

  Stream.ReadExcept(FFilters, 1, 'Error reading filters');
  if FFilters > 32 then
    raise Exception.Create('filter pipeline message has too many filters');

  for Index := 0 to FFilters - 1 do
  begin
    Stream.ReadExcept(FilterIdentificationValue, 2, 'Error reading filter identification value');
    if not FilterIdentificationValue in [1, 2] then
      raise Exception.Create('Unsupported filter');
    Stream.ReadExcept(Flags, 2, 'Error reading flags');
    Stream.ReadExcept(NumberClientDataValues, 2, 'Error reading number client data values');
    for ValueIndex := 0 to NumberClientDataValues - 1 do
      Stream.ReadExcept(ClientData, 4, 'Error reading client data');
  end;
end;


{ THdfAttribute }

constructor THdfAttribute.Create(Name: UTF8String);
begin
  FName := Name;
  FStream := TMemoryStream.Create;
end;

function THdfAttribute.GetValueAsString: UTF8String;
var
  StringStream: TStringStream;
begin
  if FStream.Size = 0 then
  begin
    Result := '';
    Exit;
  end;

  StringStream := TStringStream.Create;
  try
    FStream.Position := 0;
    StringStream.CopyFrom(FStream, FStream.Size);
    Result := StringStream.DataString;
  finally
    StringStream.Free;
  end;
end;

procedure THdfAttribute.SetValueAsString(const Value: UTF8String);
var
  StringStream: TStringStream;
begin
  StringStream := TStringStream.Create(string(Value));
  try
    FStream.Clear;
    FStream.CopyFrom(StringStream, StringStream.Size);
  finally
    StringStream.Free;
  end;
end;

{ THdfMessageAttribute }

procedure THdfMessageAttribute.ReadData(Stream: TStream; Attribute: THdfAttribute);
var
  Name: UTF8String;
  Value: Integer;
  Dimension: Integer;
  EndAddress: Integer;
begin
  case FDatatypeMessage.DataClass of
    3:
      begin
        SetLength(Name, FDatatypeMessage.Size);
        Stream.ReadExcept(Name[1], FDatatypeMessage.Size, 'Error reading string');
        Attribute.ValueAsString := Name;
      end;
    6:
      begin
        // TODO
        Stream.Seek(FDatatypeMessage.Size, soFromCurrent);
      end;
    7:
      begin
        Stream.ReadExcept(Value, 4, 'Error reading value');
        // TODO
      end;
    9:
      begin
        Stream.ReadExcept(Dimension, 4, 'Error reading dimension');
        Stream.ReadExcept(EndAddress, 4, 'Error reading end address');

        Stream.ReadExcept(Value, 4, 'Error reading value');
        Stream.ReadExcept(Value, 4, 'Error reading value');
        // TODO
      end;
    else
      raise Exception.Create('Error: unknown data class');
  end;
end;

procedure THdfMessageAttribute.ReadDataDimension(Stream: TStream;
  Attribute: THdfAttribute; Dimension: Integer);
var
  Index: Integer;
begin
  if Length(FDataspaceMessage.FDimensionSize) > 0 then
    for Index := 0 to FDataspaceMessage.FDimensionSize[0] - 1 do
    begin
      if (1 < FDataspaceMessage.Dimensionality) then
        ReadDataDimension(Stream, Attribute, Dimension + 1)
      else
        ReadData(Stream, Attribute);
    end;
end;

procedure THdfMessageAttribute.LoadFromStream(Stream: TStream);
var
  Attribute: THdfAttribute;
begin
  inherited;

  // check version
  if FVersion <> 3 then
    raise Exception.Create('Unsupported version of group info message');

  // read flags
  Stream.ReadExcept(FFlags, 1, 'Error reading flags');

  Stream.ReadExcept(FNameSize, 2, 'Error reading name size');
  Stream.ReadExcept(FDatatypeSize, 2, 'Error reading datatype size');
  Stream.ReadExcept(FDataspaceSize, 2, 'Error reading dataspace size');
  Stream.ReadExcept(FEncoding, 1, 'Error reading encoding');

  SetLength(FName, FNameSize);
  Stream.ReadExcept(FName[1], FNameSize, 'Error reading name');

  FDatatypeMessage := THdfMessageDataType.Create(Superblock, DataObject);
  FDatatypeMessage.LoadFromStream(Stream);

  FDataspaceMessage := THdfMessageDataSpace.Create(Superblock, DataObject);
  FDataspaceMessage.LoadFromStream(Stream);

  Attribute := THdfAttribute.Create(FName);
  DataObject.AddAttribute(Attribute);

  if FDataspaceMessage.Dimensionality = 0 then
    ReadData(Stream, Attribute)
  else
    ReadDataDimension(Stream, Attribute, 0);
end;

{ THdfMessageHeaderContinuation }

procedure THdfMessageHeaderContinuation.LoadFromStream(Stream: TStream);
var
  StreamPos: Integer;
  Signature: THdfSignature;
begin
  Stream.ReadExcept(FOffset, Superblock.OffsetSize, 'Error reading offset');
  Stream.ReadExcept(FLength, Superblock.LengthsSize, 'Error reading length');

  StreamPos := Stream.Position;
  Stream.Position := FOffset;

  // read signature
  Stream.ReadExcept(Signature[0], 4, 'Error reading signature');
  if Signature <> 'OCHK' then
    raise Exception.CreateFmt('Wrong signature (%s)', [string(Signature)]);

  DataObject.ReadObjectHeaderMessages(Stream, FOffset + FLength);

  Stream.Position := StreamPos;
end;


{ THdfMessageAttributeInfo }

procedure THdfMessageAttributeInfo.LoadFromStream(Stream: TStream);
begin
  inherited;

  // check version
  if FVersion <> 0 then
    raise Exception.Create('Unsupported version of attribute info message');

  // read flags
  Stream.ReadExcept(FFlags, 1, 'Error reading flags');

  if (FFlags and 1) <> 0 then
    Stream.ReadExcept(FMaximumCreationIndex, 2, 'Error reading maximum creation index');

  Stream.ReadExcept(FFractalHeapAddress, SuperBlock.OffsetSize, 'Error reading fractal heap address');
  Stream.ReadExcept(FAttributeNameBTreeAddress, SuperBlock.OffsetSize, 'Error reading attribute name B-tree address');

  if (FFlags and 2) <> 0 then
    Stream.ReadExcept(FAttributeOrderBTreeAddress, SuperBlock.OffsetSize, 'Error reading attribute order B-tree address');
end;


{ THdfCustomBlock }

constructor THdfCustomBlock.Create(SuperBlock: THdfSuperBlock;
  FractalHeap: THdfFractalHeap; DataObject: THdfDataObject);
begin
  FSuperBlock := SuperBlock;
  FFractalHeap := FractalHeap;
  FDataObject := DataObject;
end;

procedure THdfCustomBlock.LoadFromStream(Stream: TStream);
begin
  // read signature
  Stream.ReadExcept(FSignature[0], 4, 'Error reading signature');
  if FSignature <> GetSignature then
    raise Exception.CreateFmt('Wrong signature (%s)', [string(FSignature)]);

  // read version
  Stream.ReadExcept(FVersion, 1, 'Error reading version');
  if FVersion <> 0 then
    raise Exception.Create('Unsupported version of link info message');

  // read heap header address
  Stream.ReadExcept(FHeapHeaderAddress, SuperBlock.OffsetSize, 'Error reading heap header address');

  // read block offset
  FBlockOffset := 0;
  Stream.ReadExcept(FBlockOffset, (FFractalHeap.MaximumHeapSize + 7) div 8, 'Error reading block offset');
end;

procedure THdfCustomBlock.SaveToStream(Stream: TStream);
begin
  raise Exception.Create('Not yet implemented');
end;


{ THdfDirectBlock }

class function THdfDirectBlock.GetSignature: THdfSignature;
begin
  Result := 'FHDB';
end;

procedure THdfDirectBlock.LoadFromStream(Stream: TStream);
var
  Size: Integer;
  OffsetSize, LengthSize: Int64;
  TypeAndVersion: Byte;
  OffsetX, LengthX: Int64;
  Temp: Int64;
  Name, Value: AnsiString;
  Attribute: THdfAttribute;
  HeapHeaderAddress: Int64;
  StreamPos: Int64;
  SubDataObject: THdfDataObject;
begin
  inherited;

  if (FFractalHeap.Flags and 2) <> 0 then
    Stream.ReadExcept(FChecksum, 4, 'Error reading checksum');

  OffsetSize := Ceil(log2(FFractalHeap.MaximumHeapSize) / 8);
  if (FFractalHeap.MaximumDirectBlockSize < FFractalHeap.MaximumSize) then
    LengthSize := Ceil(log2(FFractalHeap.MaximumDirectBlockSize) / 8)
  else
    LengthSize := Ceil(log2(FFractalHeap.MaximumSize) / 8);

  repeat
    Stream.ReadExcept(TypeAndVersion, 1, 'Error reading type and version');

    OffsetX := 0;
    LengthX := 0;
    Stream.Read(OffsetX, OffsetSize);
    Stream.Read(LengthX, LengthSize);

    if (TypeAndVersion = 3) then
    begin
      Temp := 0;
      Stream.ReadExcept(Temp, 5, 'Error reading magic');
      if Temp <> $40008 then
        raise Exception.Create('Unsupported values');

      SetLength(Name, LengthX);
      Stream.ReadExcept(Name[1], LengthX, 'Error reading name');

      Temp := 0;
      Stream.ReadExcept(Temp, 4, 'Error reading magic');
      if (Temp <> $13) then
        raise Exception.Create('Unsupported values');

      Stream.ReadExcept(LengthX, 2, 'Error reading length');
      Temp := 0;
      Stream.ReadExcept(Temp, 6, 'Error reading unknown value');
      if Temp = $20200 then
      begin
          
      end
      else if Temp = $20000 then
      begin
        SetLength(Value, LengthX);
        Stream.ReadExcept(Value[1], LengthX, 'Error reading value');
      end
      else if Temp = $20000020000 then
      begin 
        Value := '';
      end;

      Attribute := THdfAttribute.Create(Name);
      Attribute.ValueAsString := Value;

      FDataObject.AddAttribute(Attribute);
    end
    else
    if (TypeAndVersion = 1) then
    begin
      Temp := 0;
      Stream.ReadExcept(Temp, 6, 'Error reading magic');
      if Temp <> 0 then
        raise Exception.Create('FHDB type 1 unsupported values');

      // read name  
      Stream.Read(LengthX, 1);
      SetLength(Name, LengthX);
      Stream.ReadExcept(Name[1], LengthX, 'Error reading name');

      // read heap header address
      Stream.ReadExcept(HeapHeaderAddress, SuperBlock.OffsetSize, 'Error reading heap header address');

      StreamPos := Stream.Position;
      
      Stream.Position := HeapHeaderAddress;

      SubDataObject := THdfDataObject.Create(SuperBlock, Name);
      SubDataObject.LoadFromStream(Stream);

      FDataObject.AddDataObject(SubDataObject);

      Stream.Position := StreamPos;
    end;
  until TypeAndVersion = 0;
end;


{ THdfIndirectBlock }

constructor THdfIndirectBlock.Create(SuperBlock: THdfSuperBlock;
  FractalHeap: THdfFractalHeap; DataObject: THdfDataObject);
begin
  inherited Create(SuperBlock, FractalHeap, DataObject);

  FInitialBlockSize := FractalHeap.StartingBlockSize;
end;

class function THdfIndirectBlock.GetSignature: THdfSignature;
begin
  Result := 'FHIB';
end;

procedure THdfIndirectBlock.LoadFromStream(Stream: TStream);
var
  Size: Integer;
  RowsCount: Integer;
  k, n: Integer;
  ChildBlockAddress: Int64;
  SizeOfFilteredDirectBlock: Int64;
  FilterMaskForDirectBlock: Integer;
  StreamPosition: Int64;
  Block: THdfCustomBlock;
begin
  inherited;

  if FBlockOffset <> 0 then
    raise Exception.Create('Only a block offset of 0 is supported so far');

  // The number of rows of blocks, nrows, in an indirect block of size iblock_size is given by the following expression:
  RowsCount := Round(log2(FInitialBlockSize) - log2(FFractalHeap.StartingBlockSize)) + 1;

  // The maximum number of rows of direct blocks, max_dblock_rows, in any indirect block of a fractal heap is given by the following expression: */
  FMaximumNumberOfDirectBlockRows := Round(log2(FFractalHeap.MaximumDirectBlockSize)
      - log2(FFractalHeap.StartingBlockSize)) + 2;

  // Using the computed values for nrows and max_dblock_rows, along with the Width of the doubling table, the number of direct and indirect block entries (K and N in the indirect block description, below) in an indirect block can be computed:
  if (RowsCount < FMaximumNumberOfDirectBlockRows) then
    k := RowsCount * FFractalHeap.TableWidth
  else
    k := FMaximumNumberOfDirectBlockRows * FFractalHeap.TableWidth;

  // If nrows is less than or equal to max_dblock_rows, N is 0. Otherwise, N is simply computed:
  n := k - (FMaximumNumberOfDirectBlockRows * FFractalHeap.TableWidth);

  while (k > 0) do
  begin
    ChildBlockAddress := 0;
    Stream.ReadExcept(ChildBlockAddress, SuperBlock.OffsetSize, 'Error reading child direct block address');
    if (FFractalHeap.EncodedLength > 0) then
    begin
      Stream.ReadExcept(SizeOfFilteredDirectBlock, SuperBlock.LengthsSize, 'Error reading filtered direct block');
      Stream.ReadExcept(FilterMaskForDirectBlock, 4, 'Error reading filter mask');
    end;

    if (ChildBlockAddress > 0) and (ChildBlockAddress < SuperBlock.EndOfFileAddress) then
    begin
      StreamPosition := Stream.Position;
      Stream.Position := ChildBlockAddress;

      Block := THdfDirectBlock.Create(SuperBlock, FFractalHeap, FDataObject);
      Block.LoadFromStream(Stream);

      Stream.Position := StreamPosition;
    end;
    Dec(k);
  end;

  while (n > 0) do
  begin
    ChildBlockAddress := 0;
    Stream.ReadExcept(ChildBlockAddress, SuperBlock.OffsetSize, 'Error reading child direct block address');

    if (ChildBlockAddress > 0) and (ChildBlockAddress < SuperBlock.EndOfFileAddress) then
    begin
      StreamPosition := Stream.Position;
      Stream.Position := ChildBlockAddress;

      Block := THdfInDirectBlock.Create(SuperBlock, FFractalHeap, FDataObject);
      Block.LoadFromStream(Stream);

      Stream.Position := StreamPosition;
    end;
    Dec(n);
  end;
end;


{ THdfFractalHeap }

constructor THdfFractalHeap.Create(SuperBlock: THdfSuperBlock; DataObject: THdfDataObject);
begin
  FSuperBlock := SuperBlock;
  FDataObject := DataObject;
end;

procedure THdfFractalHeap.LoadFromStream(Stream: TStream);
var
  Block: THdfCustomBlock;
begin
  // read signature
  Stream.ReadExcept(FSignature[0], 4, 'Error reading signature');
  if FSignature <> 'FRHP' then
    raise Exception.CreateFmt('Wrong signature (%s)', [string(FSignature)]);

  // read version
  Stream.ReadExcept(FVersion, 1, 'Error reading version');
  if FVersion <> 0 then
    raise Exception.Create('Unsupported version of link info message');

  Stream.ReadExcept(FHeapIdLength, 2, 'Error reading heap ID length');
  Stream.ReadExcept(FEncodedLength, 2, 'Error reading I/O filters'' encoded length');
  Stream.ReadExcept(FFlags, 1, 'Error reading flags');

  Stream.ReadExcept(FMaximumSize, 4, 'Error reading maximum size');
  Stream.ReadExcept(FNextHugeID, SuperBlock.LengthsSize, 'Error reading next huge ID');
  Stream.ReadExcept(FBtreeAddresses, SuperBlock.OffsetSize, 'Error reading Btree Addresses');
  Stream.ReadExcept(FAmountFreeSpace, SuperBlock.LengthsSize, 'Error reading amount of free space');
  Stream.Read(FAddressManagedBlock, SuperBlock.OffsetSize);
  Stream.Read(FAmountManagedSpace, SuperBlock.LengthsSize);
  Stream.Read(FAmountAllocatedManagedSpace, SuperBlock.LengthsSize);
  Stream.Read(FOffsetDirectBlockAllocation, SuperBlock.LengthsSize);
  Stream.Read(FNumberOfManagedObjects, SuperBlock.LengthsSize);
  Stream.Read(FSizeOfHugeObjects, SuperBlock.LengthsSize);
  Stream.Read(FNumberOfHugeObjects, SuperBlock.LengthsSize);
  Stream.Read(FSizeOfTinyObjects, SuperBlock.LengthsSize);
  Stream.Read(FNumberOfTinyObjects, SuperBlock.LengthsSize);
  Stream.Read(FTableWidth, 2);
  Stream.Read(FStartingBlockSize, SuperBlock.LengthsSize);
  Stream.Read(FMaximumDirectBlockSize, SuperBlock.LengthsSize);
  Stream.Read(FMaximumHeapSize, 2);
  Stream.Read(FStartingNumber, 2);
  Stream.Read(FAddressOfRootBlock, SuperBlock.OffsetSize);
  Stream.Read(FCurrentNumberOfRows, 2);
  if FEncodedLength > 0 then
  begin
    Stream.Read(FSizeOfFilteredRootDirectBlock, SuperBlock.LengthsSize);
    Stream.Read(FIOFilterMask, 4);
    // Stream.Read(FIOFilterInformation,
  end;
//  Stream.Read(FChecksum: Integer;

  if (FNumberOfHugeObjects > 0) then
    raise Exception.Create('Cannot handle huge objects');

  if (FNumberOfTinyObjects > 0) then
    raise Exception.Create('Cannot handle tiny objects');

  if (FAddressOfRootBlock > 0) and (FAddressOfRootBlock < Superblock.EndOfFileAddress) then
  begin
    Stream.Position := FAddressOfRootBlock;

    if FCurrentNumberOfRows <> 0 then
      Block := THdfIndirectBlock.Create(SuperBlock, Self, FDataObject)
    else
      Block := THdfDirectBlock.Create(SuperBlock, Self, FDataObject);
    Block.LoadFromStream(Stream);
  end;
end;

procedure THdfFractalHeap.SaveToStream(Stream: TStream);
begin
  raise Exception.Create('Not yet implemented');
end;


{ THdfDataObject }

constructor THdfDataObject.Create(SuperBlock: THdfSuperBlock);
begin
  FSuperblock := SuperBlock;
  FName := '';

  // create a few default messages
  FDataType := THdfMessageDataType.Create(FSuperBlock, Self);
  FDataSpace := THdfMessageDataSpace.Create(FSuperBlock, Self);
  FLinkInfo := THdfMessageLinkInfo.Create(FSuperBlock, Self);
  FGroupInfo := THdfMessageGroupInfo.Create(FSuperBlock, Self);
  FAttributeInfo := THdfMessageAttributeInfo.Create(FSuperBlock, Self);

  FAttributesHeap := THdfFractalHeap.Create(FSuperBlock, Self);
  FObjectsHeap := THdfFractalHeap.Create(FSuperBlock, Self);

  FData := TMemoryStream.Create;
  FAttributeList := TObjectList.Create;

  FDataObjects := TObjectList.Create;
end;

procedure THdfDataObject.AddAttribute(Attribute: THdfAttribute);
begin
  FAttributeList.Add(Attribute);
end;

procedure THdfDataObject.AddDataObject(DataObject: THdfDataObject);
begin
  FDataObjects.Add(DataObject);
end;

constructor THdfDataObject.Create(SuperBlock: THdfSuperBlock; Name: UTF8String);
begin
  Create(SuperBlock);
  FName := Name;
end;

function THdfDataObject.GetAttributeListCount: Integer;
begin
  Result := FAttributeList.Count;
end;

function THdfDataObject.GetAttributeListItem(Index: Integer): THdfAttribute;
begin
  if (Index < 0) or (Index >= FAttributeList.Count) then
    raise Exception.CreateFmt('Index out of bounds (%d)', [Index]);

  Result := THdfAttribute(FAttributeList[Index]);
end;

function THdfDataObject.GetDataLayoutChunk(Index: Integer): Integer;
begin
  if (Index < 0) or (Index >= Length(FDataLayoutChunk)) then
    raise Exception.CreateFmt('Index out of bounds (%d)', [Index]);

  Result := FDataLayoutChunk[Index];
end;

function THdfDataObject.GetDataLayoutChunkCount: Integer;
begin
  Result := Length(FDataLayoutChunk);
end;

function THdfDataObject.GetDataObject(Index: Integer): THdfDataObject;
begin
  if (Index < 0) or (Index >= FDataObjects.Count) then
    raise Exception.CreateFmt('Index out of bounds (%d)', [Index]);

  Result := THdfDataObject(FDataObjects[Index]);
end;

function THdfDataObject.GetDataObjectCount: Integer;
begin
  Result := FDataObjects.Count;
end;

procedure THdfDataObject.LoadFromStream(Stream: TStream);
begin
  Stream.ReadExcept(FSignature[0], 4, 'Error reading signature');
  if FSignature <> 'OHDR' then
    raise Exception.CreateFmt('Wrong signature (%s)', [string(FSignature)]);

  // read version
  Stream.ReadExcept(FVersion, 1, 'Error reading version');
  if FVersion <> 2 then
    raise Exception.Create('Invalid verion');

  Stream.ReadExcept(FFlags, 1, 'Error reading flags');

  // eventually read time stamps
  if (FFlags and (1 shl 5)) <> 0 then
  begin
    Stream.ReadExcept(FAccessTime, 4, 'Error reading access time');
    Stream.ReadExcept(FModificationTime, 4, 'Error reading modification time');
    Stream.ReadExcept(FChangeTime, 4, 'Error reading change time');
    Stream.ReadExcept(FBirthTime, 4, 'Error reading birth time');
  end;

  // eventually skip number of attributes
  if (FFlags and (1 shl 4)) <> 0 then
  begin
    Stream.ReadExcept(FMaximumCompact, 2, 'Error reading maximum number of compact attributes');
    Stream.ReadExcept(FMinimumDense, 2, 'Error reading minimum number of dense attributes');
  end;

  Stream.ReadExcept(FChunkSize, 1 shl (FFlags and 3), 'Error reading chunk size');

  ReadObjectHeaderMessages(Stream, Stream.Position + FChunkSize);

  // parse message attribute info
  if (AttributeInfo.FractalHeapAddress > 0) and
     (AttributeInfo.FractalHeapAddress < FSuperblock.EndOfFileAddress) then
  begin
    Stream.Position := AttributeInfo.FractalHeapAddress;
    FAttributesHeap.LoadFromStream(Stream);
  end;

  // parse message link info
  if (LinkInfo.FractalHeapAddress > 0) and
     (LinkInfo.FractalHeapAddress < FSuperblock.EndOfFileAddress) then
  begin
    Stream.Position := LinkInfo.FractalHeapAddress;
    FObjectsHeap.LoadFromStream(Stream);
  end;
end;

procedure THdfDataObject.SaveToStream(Stream: TStream);
begin
  raise Exception.Create('Not yet implemented');
end;

procedure THdfDataObject.ReadObjectHeaderMessages(Stream: TStream; EndOfStream: Int64);
var
  MessageType: Byte;
  MessageSize: Word;
  MessageFlags: Byte;
  EndPos: Int64;
  DataObjectMessage: THdfDataObjectMessage;
begin
  while Stream.Position < EndOfStream - 4 do
  begin
    Stream.ReadExcept(MessageType, 1, 'Error reading message type');
    Stream.ReadExcept(MessageSize, 2, 'Error reading message size');
    Stream.ReadExcept(MessageFlags, 1, 'Error reading message flags');

    if (MessageFlags and not 5) <> 0 then
      raise Exception.Create('Unsupported OHDR message flag');

    // eventually skip creation order
    if FFlags and (1 shl 2) <> 0 then
      Stream.Seek(2, soFromCurrent);

    EndPos := Stream.Position + MessageSize;

    DataObjectMessage := nil;
    case MessageType of
      0:
        Stream.Seek(MessageSize, soFromCurrent);
      1:
        DataObjectMessage := FDataSpace;
      2:
        DataObjectMessage := FLinkInfo;
      3:
        DataObjectMessage := FDataType;
      5:
        DataObjectMessage := THdfMessageDataFill.Create(FSuperBlock, Self);
      8:
        DataObjectMessage := THdfMessageDataLayout.Create(FSuperBlock, Self);
      10:
        DataObjectMessage := FGroupInfo;
      11:
        DataObjectMessage := THdfMessageFilterPipeline.Create(FSuperBlock, Self);
      12:
        DataObjectMessage := THdfMessageAttribute.Create(FSuperBlock, Self);
      16:
        DataObjectMessage := THdfMessageHeaderContinuation.Create(FSuperBlock, Self);
      21:
        DataObjectMessage := FAttributeInfo;
      else
        raise Exception.CreateFmt('Unknown header message (%d)', [MessageType]);
    end;

    // now eventally load data object message
    if Assigned(DataObjectMessage) then
      DataObjectMessage.LoadFromStream(Stream);

    {$IFDEF IgnoreWrongPosition}
    Stream.Position := EndPos;
    {$ELSE}
    if Stream.Position <> EndPos then
      Assert(Stream.Position = EndPos);
    {$ENDIF}
  end;
end;


{ THdfFile }

procedure THdfFile.AfterConstruction;
begin
  inherited;

  FSuperBlock := THdfSuperBlock.Create;
  FDataObject := THdfDataObject.Create(FSuperblock);
end;

procedure THdfFile.LoadFromFile(Filename: TFileName);
var
  MS: TMemoryStream;
begin
  MS := TMemoryStream.Create;
  try
    MS.LoadFromFile(FileName);
    LoadFromStream(MS);
  finally
    MS.Free;
  end;
end;

procedure THdfFile.LoadFromStream(Stream: TStream);
begin
  FSuperblock.LoadFromStream(Stream);
  FDataObject.LoadFromStream(Stream);
end;

procedure THdfFile.SaveToFile(Filename: TFileName);
var
  MS: TMemoryStream;
begin
  MS := TMemoryStream.Create;
  try
    SaveToStream(MS);
    MS.SaveToFile(FileName);
  finally
    MS.Free;
  end;
end;

procedure THdfFile.SaveToStream(Stream: TStream);
begin
  raise Exception.Create('Not yet implemented');
end;

end.
