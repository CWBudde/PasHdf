unit HdfFile;

interface

uses
  Classes, SysUtils, Contnrs;

type
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

  THdfDataObjectMessage = class(TInterfacedPersistent, IStreamPersist)
  private
    FSuperBlock: THdfSuperBlock;
  protected
    FVersion: Byte;
    FFlags: Byte;
    property Superblock: THdfSuperBlock read FSuperBlock;
  public
    constructor Create(SuperBlock: THdfSuperBlock);

    procedure LoadFromStream(Stream: TStream); virtual;
    procedure SaveToStream(Stream: TStream);
  end;

  THdfMessageDataSpace = class(THdfDataObjectMessage)
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;
    
  THdfMessageLinkInfo = class(THdfDataObjectMessage)
  private
    FMaximumCreationIndex: Int64;
    FFractalHeapAddress: Int64;
    FAddressBTreeIndex: Int64;
    FAddressBTreeOrder: Int64;
  public
    procedure LoadFromStream(Stream: TStream); override;

    property FractalHeapAddress: Int64 read FFractalHeapAddress;
  end;

  THdfMessageDataType = class(THdfDataObjectMessage);
  THdfMessageDataFill = class(THdfDataObjectMessage);
  THdfMessageDataLayout = class(THdfDataObjectMessage);

  THdfMessageGroupInfo = class(THdfDataObjectMessage)
  private
    FMaximumCompact: Word;
    FMinimumDense: Word;
    FEstimatedNumberOfEntries: Word;
    FEstimatedLinkNameLength: Word;
  public
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfMessageFilterPipeline = class(THdfDataObjectMessage);

  THdfMessageAttribute = class(THdfDataObjectMessage);

  THdfMessageContinue = class(THdfDataObjectMessage);

  THdfMessageAttributeInfo = class(THdfDataObjectMessage)
  private
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
    constructor Create(SuperBlock: THdfSuperBlock);

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
    FSignature: THdfSignature;
    FVersion: Byte;
    FHeapHeaderAddress: Int64;
    FBlockOffset: Int64;
    FChecksum: Integer;
  protected
    class function GetSignature: THdfSignature; virtual; abstract;

    property SuperBlock: THdfSuperBlock read FSuperBlock;
  public
    constructor Create(SuperBlock: THdfSuperBlock; FractalHeap: THdfFractalHeap); virtual;

    procedure LoadFromStream(Stream: TStream); virtual;
    procedure SaveToStream(Stream: TStream);
  end;

  THdfNameValuePair = class
    Name: string;
    Value: string;
  end;

  THdfDirectBlock = class(THdfCustomBlock)
  private
    FNameValueList: TObjectList;
  protected
    class function GetSignature: THdfSignature; override;
  public
    procedure AfterConstruction; override;
  
    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfIndirectBlock = class(THdfCustomBlock)
  private
    FInitialBlockSize: Int64;
    FMaximumNumberOfDirectBlockRows: Integer;
  protected
    class function GetSignature: THdfSignature; override;
  public
    constructor Create(SuperBlock: THdfSuperBlock; FractalHeap: THdfFractalHeap); override;

    procedure LoadFromStream(Stream: TStream); override;
  end;

  THdfDataObject = class(TInterfacedPersistent, IStreamPersist)
  private
    FName: string;
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

    FLinkInfo: THdfMessageLinkInfo;
    FGroupInfo: THdfMessageGroupInfo;
    FAttributeInfo: THdfMessageAttributeInfo;
    procedure ReadObjectHeaderMessages(Stream: TStream);
  protected
    property Superblock: THdfSuperBlock read FSuperBlock;
  public
    constructor Create(SuperBlock: THdfSuperBlock); overload;
    constructor Create(SuperBlock: THdfSuperBlock; Name: string); overload;

    procedure LoadFromStream(Stream: TStream);
    procedure SaveToStream(Stream: TStream);

    property LinkInfo: THdfMessageLinkInfo read FLinkInfo;
    property GroupInfo: THdfMessageGroupInfo read FGroupInfo;
    property AttributeInfo: THdfMessageAttributeInfo read FAttributeInfo;
  end;

  THdfFile = class(TInterfacedPersistent, IStreamPersist)
  private
    FSuperBlock: THdfSuperBlock;
    FDataObject: THdfDataObject;
    FAttributesHeap: THdfFractalHeap;
    FObjectsHeap: THdfFractalHeap;
  public
    procedure AfterConstruction; override;

    procedure LoadFromStream(Stream: TStream);
    procedure SaveToStream(Stream: TStream);

    procedure LoadFromFile(Filename: TFileName);
    procedure SaveToFile(Filename: TFileName);
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

constructor THdfDataObjectMessage.Create(SuperBlock: THdfSuperBlock);
begin
  FSuperBlock := SuperBlock;
end;

procedure THdfDataObjectMessage.LoadFromStream(Stream: TStream);
begin
  // read version
  Stream.ReadExcept(FVersion, 1, 'Error reading version');
  if FVersion <> 0 then
    raise Exception.Create('Unsupported version of link info message');

  // read flags
  Stream.ReadExcept(FFlags, 1, 'Error reading flags');
end;

procedure THdfDataObjectMessage.SaveToStream(Stream: TStream);
begin
  raise Exception.Create('Not yet implemented');
end;


{ THdfMessageDataSpace }

procedure THdfMessageDataSpace.LoadFromStream(Stream: TStream);
begin
  inherited;

end;


{ THdfMessageLinkInfo }

procedure THdfMessageLinkInfo.LoadFromStream(Stream: TStream);
begin
  inherited;

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



{ THdfMessageAttributeInfo }

procedure THdfMessageAttributeInfo.LoadFromStream(Stream: TStream);
begin
  inherited;

  if (FFlags and 1) <> 0 then
    Stream.ReadExcept(FMaximumCreationIndex, 2, 'Error reading maximum creation index');

  Stream.ReadExcept(FFractalHeapAddress, SuperBlock.OffsetSize, 'Error reading fractal heap address');
  Stream.ReadExcept(FAttributeNameBTreeAddress, SuperBlock.OffsetSize, 'Error reading attribute name B-tree address');

  if (FFlags and 2) <> 0 then
    Stream.ReadExcept(FAttributeOrderBTreeAddress, SuperBlock.OffsetSize, 'Error reading attribute order B-tree address');
end;


{ THdfCustomBlock }

constructor THdfCustomBlock.Create(SuperBlock: THdfSuperBlock;
  FractalHeap: THdfFractalHeap);
begin
  FSuperBlock := SuperBlock;
  FFractalHeap := FractalHeap;
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

procedure THdfDirectBlock.AfterConstruction;
begin
  inherited;

  FNameValueList := TObjectList.Create;
end;

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
  NameValuePair: THdfNameValuePair;
  HeapHeaderAddress: Int64;
  StreamPos: Int64;
  DataObject: THdfDataObject;
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

      NameValuePair := THdfNameValuePair.Create;
      NameValuePair.Name := Name;
      NameValuePair.Value := Value;

      FNameValueList.Add(NameValuePair);
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
      
(*
			log("\nfractal head type 1 length %4lX name %s address %lX\n", length, name, heap_header_address);

			dir = malloc(sizeof(struct DIR));
			dir->next = dataobject->directory;
			dataobject->directory = dir;
*)

      Stream.Position := HeapHeaderAddress;

      DataObject := THdfDataObject.Create(SuperBlock, Name);
      DataObject.LoadFromStream(Stream);
      
			//dataobjectRead(reader, &dir->dataobject, name);

      Stream.Position := StreamPos;
    end;
  until TypeAndVersion = 0;
end;


{ THdfIndirectBlock }

constructor THdfIndirectBlock.Create(SuperBlock: THdfSuperBlock;
  FractalHeap: THdfFractalHeap);
begin
  FSuperBlock := SuperBlock;
  FFractalHeap := FractalHeap;
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
  DirectBlock: THdfDirectBlock;
  IndirectBlock: THdfIndirectBlock;
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

      DirectBlock := THdfDirectBlock.Create(SuperBlock, FFractalHeap);
      DirectBlock.LoadFromStream(Stream);

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

      IndirectBlock := THdfInDirectBlock.Create(SuperBlock, FFractalHeap);
      IndirectBlock.LoadFromStream(Stream);

      Stream.Position := StreamPosition;
		end;

		Dec(n);
	end;
end;


{ THdfFractalHeap }

constructor THdfFractalHeap.Create(SuperBlock: THdfSuperBlock);
begin
  FSuperBlock := SuperBlock;
end;

procedure THdfFractalHeap.LoadFromStream(Stream: TStream);
var
  DirectBlock: THdfDirectBlock;
  InDirectBlock: THdfIndirectBlock;
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
  Stream.Read(FNextHugeID, SuperBlock.LengthsSize);
  Stream.Read(FBtreeAddresses, SuperBlock.OffsetSize);
  Stream.Read(FAmountFreeSpace, SuperBlock.LengthsSize);
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
    begin
      InDirectBlock := THdfIndirectBlock.Create(SuperBlock, Self);
      InDirectBlock.LoadFromStream(Stream);
    end
    else
    begin
      DirectBlock := THdfDirectBlock.Create(SuperBlock, Self);
      DirectBlock.LoadFromStream(Stream);
    end;
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
  FLinkInfo := THdfMessageLinkInfo.Create(FSuperBlock);
  FGroupInfo := THdfMessageGroupInfo.Create(FSuperBlock);
  FAttributeInfo := THdfMessageAttributeInfo.Create(FSuperBlock);
end;

constructor THdfDataObject.Create(SuperBlock: THdfSuperBlock; Name: string);
begin
  Create(SuperBlock);
  FName := Name;
end;

procedure THdfDataObject.LoadFromStream(Stream: TStream);
var
  SubStream: TMemoryStream;
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

  SubStream := TMemoryStream.Create;
  try
    SubStream.CopyFrom(Stream, FChunkSize - 4);
    SubStream.Seek(0, soBeginning);
    ReadObjectHeaderMessages(SubStream);
  finally
    SubStream.Free;
  end;
end;

procedure THdfDataObject.SaveToStream(Stream: TStream);
begin
  raise Exception.Create('Not yet implemented');
end;

procedure THdfDataObject.ReadObjectHeaderMessages(Stream: TStream);
var
  MessageType: Byte;
  MessageSize: Word;
  MessageFlags: Byte;
  EndPos: Int64;
  DataObjectMessage: THdfDataObjectMessage;
begin
  while Stream.Position < Stream.Size - 4 do
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
        DataObjectMessage := THdfMessageDataSpace.Create(FSuperBlock);
      2:
        DataObjectMessage := FLinkInfo;
      3:
        DataObjectMessage := THdfMessageDataType.Create(FSuperBlock);
      5:
        DataObjectMessage := THdfMessageDataFill.Create(FSuperBlock);
      8:
        DataObjectMessage := THdfMessageDataLayout.Create(FSuperBlock);
      10:
        DataObjectMessage := FGroupInfo;
      11:
        DataObjectMessage := THdfMessageFilterPipeline.Create(FSuperBlock);
      12:
        DataObjectMessage := THdfMessageAttribute.Create(FSuperBlock);
      16:
        DataObjectMessage := THdfMessageContinue.Create(FSuperBlock);
      21:
        DataObjectMessage := FAttributeInfo;
      else
        raise Exception.CreateFmt('Unknown header message (%d)', [MessageType]);
    end;

    // now eventally load data object message
    if Assigned(DataObjectMessage) then
      DataObjectMessage.LoadFromStream(Stream);

    Assert(Stream.Position = EndPos);
  end;
end;


{ THdfFile }

procedure THdfFile.AfterConstruction;
begin
  inherited;

  FSuperBlock := THdfSuperBlock.Create;
  FDataObject := THdfDataObject.Create(FSuperblock);

  FAttributesHeap := THdfFractalHeap.Create(FSuperBlock);
  FObjectsHeap := THdfFractalHeap.Create(FSuperBlock);
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

  // parse message attribute info
  if (FDataObject.AttributeInfo.FractalHeapAddress > 0) and
     (FDataObject.AttributeInfo.FractalHeapAddress < FSuperblock.EndOfFileAddress) then
  begin
    Stream.Position := FDataObject.AttributeInfo.FractalHeapAddress;
    FAttributesHeap.LoadFromStream(Stream);
  end;

  // parse message link info
  if (FDataObject.LinkInfo.FractalHeapAddress > 0) and
     (FDataObject.LinkInfo.FractalHeapAddress < FSuperblock.EndOfFileAddress) then
  begin
    Stream.Position := FDataObject.LinkInfo.FractalHeapAddress;
    FObjectsHeap.LoadFromStream(Stream);
  end;
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
