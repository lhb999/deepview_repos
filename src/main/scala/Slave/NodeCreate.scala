package Slave
// LeafNode 생성
sealed trait LeafNode_Create{
  def pointer : Any
  def value : Any
  def Size : Int
  var header = new LeafHeader
  var entry = new LeafEntry(pointer,value,Size)
  var margin = new Margin
}

class LeafHeader extends  Serializable {
  var Flag:Boolean = true
  var numOfdata:Int = 1
  var NextNode:LeafNode = null
}
class LeafEntry(pointer : Any, value :Any, Size : Int) extends Serializable {
  var data = new Array[Any](Size)
  var pointers  = new Array[Any](Size)
  data(0) = value
  pointers(0) = pointer
}

// InternalNode 생성
sealed trait InternalNode_Create{
  def pointer : Any
  def value : Any
  def Size : Int

  var header = new InternalHeader
  var entry = new InternalEntry(pointer,value,Size)
  var margin = new Margin
}

class InternalHeader extends Serializable {
  var Flag:Boolean = false
  var numOfdata:Int = 1
  var PartionNumver:Int = 0
}
class InternalEntry(pointer : Any,value :Any, Size : Int) extends Serializable {
  var data = new Array[Any](Size)
  var pointers = new Array[Any](Size+1)
  data(0) = value
  pointers(0) = pointer
}

class Margin extends Serializable {
  var margin:Byte = 0
}
case class LeafNode( pointer : Any, value : Any, Size : Int) extends LeafNode_Create
case class InternalNode( pointer : Any, value : Any, Size : Int) extends InternalNode_Create

//leafNode Data 삽입시 사용
case class NodeAdd(Node:LeafNode, pointer: Any, value: Any) extends Serializable {
  Node.entry.data(Node.header.numOfdata) = value
  Node.entry.pointers(Node.header.numOfdata) = pointer
  Node.header.numOfdata = Node.header.numOfdata + 1
}

// internalNode Data 삽입시 사용
case class NodeAdd2(Node:InternalNode, pointer: Any, value: Any) extends Serializable {
  Node.entry.data(Node.header.numOfdata) = value
  Node.entry.pointers(Node.header.numOfdata) = pointer
  Node.header.numOfdata = Node.header.numOfdata + 1
}


// BulkLoading할 때 Internal Node의 Data 삭제가 필요할시 사용
case class Noderemove(Node:InternalNode){
  Node.entry.data(Node.header.numOfdata-1) = null
  Node.entry.pointers(Node.header.numOfdata) = null
  Node.header.numOfdata -= 1
}


