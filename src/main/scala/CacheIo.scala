package org.wabase

import io.bullet.borer._
import io.bullet.borer.derivation.MapBasedCodecs._
import io.bullet.borer.Codec
import org.tresql.ast._
import org.tresql.metadata.{Par, Procedure, ReturnType}
import CompilerAst._

object CacheIo {
  implicit      val tableColDefCodec:   Codec[TableColDef]    = deriveCodec    [TableColDef]
  implicit lazy val exprTypeCodec:      Codec[ExprType]       = deriveCodec    [ExprType]
  implicit lazy val parCodec:           Codec[Par]            = deriveCodec    [Par]
  implicit lazy val returnTypeCodec:    Codec[ReturnType]     = deriveAllCodecs[ReturnType]
  implicit lazy val procedureCodec:     Codec[Procedure]      = deriveCodec    [Procedure]
  implicit lazy val joinCodec:          Codec[Join]           = deriveCodec    [Join]          // TODO
  implicit lazy val objCodec:           Codec[Obj]            = deriveCodec    [Obj]           // TODO
  implicit lazy val tableDefCodec:      Codec[TableDef]       = deriveCodec    [TableDef]      // TODO
  implicit lazy val identCodec:         Codec[Ident]          = deriveCodec    [Ident]         // TODO
  implicit lazy val arrCodec:           Codec[Arr]            = deriveCodec    [Arr]           // TODO
  implicit lazy val colCodec:           Codec[Col]            = deriveCodec    [Col]           // TODO
  implicit lazy val colsCodec:          Codec[Cols]           = deriveCodec    [Cols]          // TODO
  implicit lazy val deleteCodec:        Codec[Delete]         = deriveCodec    [Delete]        // TODO
  implicit lazy val colDefCodec:        Codec[ColDef]         = deriveCodec    [ColDef]        // TODO
  implicit lazy val insertCodec:        Codec[Insert]         = deriveCodec    [Insert]        // TODO
  implicit lazy val updateCodec:        Codec[Update]         = deriveCodec    [Update]        // TODO
  implicit lazy val withTableDefCodec:  Codec[WithTableDef]   = deriveCodec    [WithTableDef]  // TODO
  implicit lazy val dmlDefBaseCodec:    Codec[DMLDefBase]     = deriveAllCodecs[DMLDefBase]    // TODO
  implicit lazy val ordColCodec:        Codec[OrdCol]         = deriveCodec    [OrdCol]        // TODO
  implicit lazy val ordCodec:           Codec[Ord]            = deriveCodec    [Ord]           // TODO
  implicit lazy val funCodec:           Codec[Fun]            = deriveCodec    [Fun]           // TODO
  implicit lazy val funDefCodec:        Codec[FunDef]         = deriveCodec    [FunDef]        // TODO
  implicit lazy val filtersCodec:       Codec[Filters]        = deriveCodec    [Filters]       // TODO
  implicit lazy val grpCodec:           Codec[Grp]            = deriveCodec    [Grp]           // TODO
  implicit lazy val queryCodec:         Codec[Query]          = deriveCodec    [Query]         // TODO
  implicit lazy val binOpCodec:         Codec[BinOp]          = deriveCodec    [BinOp]         // TODO
  implicit lazy val selectDefBaseCodec: Codec[SelectDefBase]  = deriveAllCodecs[SelectDefBase] // TODO
  implicit lazy val sqlDefBaseCodec:    Codec[SQLDefBase]     = deriveAllCodecs[SQLDefBase]    // TODO
  // define explicitly empty transformer exp codec since it cannot be derived
  implicit lazy val transformerExpCodec: Codec[TransformerExp] = Codec(
    new Encoder[TransformerExp] {
      override def write(w: Writer, value: TransformerExp): Writer = ???
    },
    new Decoder[TransformerExp] {
      override def read(r: Reader): TransformerExp = ???
    }
  )
  implicit lazy val expCodec:           Codec[Exp]            = deriveAllCodecs[Exp]
  implicit lazy val varCodec:           Codec[Variable]       = deriveCodec    [Variable]
}
