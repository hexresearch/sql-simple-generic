{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules, UndecidableInstances #-}
module DB.PG.Instances where

import Control.Newtype as Newtype
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromField
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.ToField
import Data.Data
import Data.Proxy
import Data.Scientific
import Data.String
import Data.Text.Conversions
import Data.Text (Text)
import Data.Time.Calendar
import GHC.Generics
import GHC.TypeLits (KnownSymbol(..), symbolVal)
import qualified Data.Text as Text
import qualified GHC.TypeLits as T
import Text.InterpolatedString.Perl6 (qc)

import DB.PG
import Data.MOEX

instance ToText Day where
  toText d = toText (show d)

instance FromRow Symbol where
  fromRow = Symbol <$> field

deriving newtype instance ToField Symbol
deriving newtype instance FromField Symbol

deriving newtype instance ToField Volume
deriving newtype instance FromField Volume

deriving newtype instance FromField BondCouponFreq
deriving newtype instance ToField BondCouponFreq

deriving newtype instance FromField  BondIssueDate
deriving newtype instance ToField  BondIssueDate

deriving newtype instance FromField BondMatDate
deriving newtype instance ToField BondMatDate

deriving newtype instance FromField BondShortName
deriving newtype instance ToField BondShortName

deriving newtype instance FromField BondFullName
deriving newtype instance ToField BondFullName

deriving newtype instance FromField TradeAggDate
deriving newtype instance ToField TradeAggDate

deriving newtype instance FromField TradeAggVol
deriving newtype instance ToField TradeAggVol

deriving newtype instance ToField Market
deriving newtype instance FromField Market

deriving newtype instance ToField Qty
deriving newtype instance FromField Qty

deriving newtype instance FromField BondPortfolioId
deriving newtype instance ToField BondPortfolioId

deriving newtype instance FromField BondPortfolioUUID
deriving newtype instance ToField BondPortfolioUUID

deriving newtype instance FromField BondCouponDate
deriving newtype instance ToField BondCouponDate

instance ToField BondPortfolioName where
  toField (BondPortfolioName x) = toField x

instance FromField BondPortfolioName where
  fromField f x = BondPortfolioName <$> fromField f x


instance FromField BondCouponValue where
  fromField f x = (BondCouponValue <$> realToFrac) <$> (fromField @Scientific f x)

instance FromField BondCouponPercent where
  fromField f x = (BondCouponPercent <$> realToFrac) <$> (fromField @Scientific f x)

instance ToText BondCouponPercent where
  toText = toText . show . Newtype.unpack


instance ToField Price where
  toField (Price x) = toField (realToFrac x :: Scientific)


instance HasColumn "bondshortname" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondshortname" (Proxy BondShortName) where
  column _ _ = "value"

instance HasColumn "bondfullname" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondfullname" (Proxy BondFullName) where
  column _ _ = "value"

instance HasColumn "bondtradeagg" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondcouponfreq" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondcouponfreq" (Proxy BondCouponFreq) where
  column _ _ = "value"

instance HasColumn "bondissuedate" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondissuedate" (Proxy BondIssueDate) where
  column _ _ = "value"

instance HasColumn "bondmatdate" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondmatdate" (Proxy BondMatDate) where
  column _ _ = "value"

instance HasColumn "bondcouponvalue" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondcouponvalue" (Proxy BondCouponValue) where
  column _ _ = "value"

newtype TradeAggDate = TradeAggDate Day
                       deriving (Eq,Ord,Show,Data,Generic)

instance Newtype TradeAggDate Day

instance ToText TradeAggDate where
  toText = toText . Newtype.unpack

newtype TradeAggVol  = TradeAggVol (Maybe Integer)
                       deriving (Eq,Ord,Show,Data,Generic)

instance Newtype TradeAggVol (Maybe Integer)

instance ToText TradeAggVol where
  toText (TradeAggVol (Just x))  = toText (show x)
  toText (TradeAggVol Nothing) = ""

newtype TradeAggNum  = TradeAggNum (Maybe Int)
                       deriving (Eq,Ord,Show,Data,Generic)

instance Newtype TradeAggNum (Maybe Int)

instance ToText TradeAggNum where
  toText (TradeAggNum (Just x)) = toText (show x)
  toText (TradeAggNum Nothing)  = ""

instance HasColumn "bondtradeagg" (Proxy TradeAggDate) where
  column _ _  = "day"

instance HasColumn "bondtradeagg" (Proxy TradeAggVol) where
  column _ _  = "vol"

instance HasColumn "bondtradeagg" (Proxy TradeAggNum) where
  column _ _  = "trades"

instance HasColumn "vbonddate" (Proxy Symbol) where
  column _ _  = "symbol"

instance HasColumn "vbonddate" (Proxy BondIssueDate) where
  column _ _  = "issuedate"

instance HasColumn "vbonddate" (Proxy BondMatDate) where
  column _ _  = "matdate"

instance HasColumn "vbonddate" (Proxy BondCouponFreq) where
  column _ _  = "freq"

instance HasColumn "vbonddate" (Proxy BondCouponValue) where
  column _ _  = "coupon"

instance HasColumn "vbonddate" (Proxy BondShortName) where
  column _ _  = "shortname"

instance HasColumn "bondportfolio" (Proxy BondPortfolioId) where
  column _ _ = "id"

instance FromRow BondPortfolioId where
  fromRow = BondPortfolioId <$> field


instance HasColumn "bondportfolioposition" (Proxy BondPortfolioId) where
  column _ _ = "portfolioid"

instance HasColumn "bondportfolioposition" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondportfolioposition" (Proxy Qty) where
  column _ _ = "qty"


instance FromRow BondPortfolioUUID where
  fromRow = BondPortfolioUUID <$> field

instance FromField BondCouponPayment where
  fromField t x = (BondCouponPayment . realToFrac) <$> (fromField @Scientific t x)

instance ToField BondCouponPayment where
  toField  = toField . realToFrac . Newtype.unpack

instance FromRow BondCouponPayment where
  fromRow = (BondCouponPayment . realToFrac) <$> field

instance HasColumn "bondportfolio" (Proxy BondPortfolioUUID) where
  column _ _ = "uuid"


instance HasColumn "bondportfolioname" (Proxy BondPortfolioId) where
  column _ _ = "portfolioid"

instance HasColumn "bondportfolioname" (Proxy BondPortfolioName) where
  column _ _ = "name"

-- vbondportfoliodates

instance HasColumn "vbondportfoliodates" (Proxy BondPortfolioId) where
  column _ _ = "portfolioid"

instance HasColumn "vbondportfoliodates" (Proxy BondPortfolioUUID) where
  column _ _ = "portfoliouuid"

instance HasColumn "vbondportfoliodates" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "vbondportfoliodates" (Proxy Qty) where
  column _ _ = "qty"

instance HasColumn "vbondportfoliodates" (Proxy BondIssueDate) where
  column _ _ = "issuedate"

instance HasColumn "vbondportfoliodates" (Proxy BondMatDate) where
  column _ _ = "matdate"

instance HasColumn "vbondportfoliodates" (Proxy BondCouponFreq) where
  column _ _ = "freq"

instance HasColumn "vbondportfoliodates" (Proxy BondCouponValue) where
  column _ _ = "coupon"

instance HasColumn "vbondportfoliodates" (Proxy BondCouponPayment) where
  column _ _ = "payment"


instance HasColumn "vbondinfo" (Proxy Symbol) where
  column _ _  = "symbol"

instance HasColumn "vbondinfo" (Proxy BondIssueDate) where
  column _ _  = "issuedate"

instance HasColumn "vbondinfo" (Proxy BondMatDate) where
  column _ _  = "matdate"

instance HasColumn "vbondinfo" (Proxy BondCouponFreq) where
  column _ _  = "freq"

instance HasColumn "vbondinfo" (Proxy BondCouponValue) where
  column _ _  = "coupon"

instance HasColumn "vbondinfo" (Proxy BondCouponPercent) where
  column _ _  = "coupon_percent"

instance HasColumn "vbondinfo" (Proxy BondCurrency) where
  column _ _  = "currency"

instance HasColumn "vbondinfo" (Proxy BondShortName) where
  column _ _  = "shortname"

instance HasColumn "vbondinfo" (Proxy BondFullName) where
  column _ _  = "fullname"

instance HasColumn "bondcoupondate" (Proxy Symbol) where
  column _ _  = "symbol"

instance HasColumn "bondcoupondate" (Proxy BondCouponDate) where
  column _ _  = "day"



instance FromRow BondCouponDate where
  fromRow = BondCouponDate <$> field


-- vbondportfolio
instance HasColumn "vbondportfolio" (Proxy BondPortfolioUUID) where
  column _ _ = "uuid"

instance HasColumn "vbondportfolio" (Proxy BondPortfolioName) where
  column _ _ = "name"


