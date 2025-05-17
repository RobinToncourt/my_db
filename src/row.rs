use std::ops::Range;

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq)]
pub enum DeserializeError {
    InvalidBytesSlice(usize),
    FromUtf8Error(std::string::FromUtf8Error),
    TryFromSliceError {
        name: String,
        expected_size: usize,
        obtained_size: usize,
    },
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq, Clone)]
pub struct Id(usize);
impl Id {
    pub const MAX_SIZE: usize = 8;

    pub fn new(id: usize) -> Self {
        Self(id)
    }
}
impl std::convert::From<Id> for [u8; Id::MAX_SIZE] {
    fn from(id: Id) -> [u8; Id::MAX_SIZE] {
        id.to_be_bytes()
    }
}
impl std::convert::From<[u8; Self::MAX_SIZE]> for Id {
    fn from(arr: [u8; Self::MAX_SIZE]) -> Self {
        Self(usize::from_be_bytes(arr))
    }
}
impl std::ops::Deref for Id {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq, Clone)]
pub struct Username(String);
impl Username {
    pub const MAX_SIZE: usize = 32;

    pub fn new(username: String) -> Self {
        Self(username)
    }
}
impl std::convert::From<Username> for [u8; Username::MAX_SIZE] {
    fn from(username: Username) -> [u8; Username::MAX_SIZE] {
        let mut bytes = username.0.into_bytes();
        bytes.resize_with(Username::MAX_SIZE, || 0);
        // La liste est garantie d'être Username::MAX_SIZE.
        #[allow(clippy::unwrap_used)]
        <[u8; Username::MAX_SIZE]>::try_from(bytes).unwrap()
    }
}
impl std::convert::TryFrom<[u8; Self::MAX_SIZE]> for Username {
    type Error = DeserializeError;

    fn try_from(arr: [u8; Self::MAX_SIZE]) -> Result<Self, Self::Error> {
        let username = String::from_utf8(Vec::<u8>::from(arr))
            .map_err(DeserializeError::FromUtf8Error)?
            .trim_matches(char::from(0))
            .to_string();

        Ok(Username(username))
    }
}
impl std::ops::Deref for Username {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq, Clone)]
pub struct Email(String);
impl Email {
    pub const MAX_SIZE: usize = 255;

    pub fn new(email: String) -> Self {
        Self(email)
    }
}
impl std::convert::From<Email> for [u8; Email::MAX_SIZE] {
    fn from(email: Email) -> [u8; Email::MAX_SIZE] {
        let mut bytes = email.0.into_bytes();
        bytes.resize_with(Email::MAX_SIZE, || 0);
        // La liste est garantie d'être Email::MAX_SIZE.
        #[allow(clippy::unwrap_used)]
        <[u8; Email::MAX_SIZE]>::try_from(bytes).unwrap()
    }
}
impl std::convert::TryFrom<[u8; Self::MAX_SIZE]> for Email {
    type Error = DeserializeError;

    fn try_from(arr: [u8; Self::MAX_SIZE]) -> Result<Self, Self::Error> {
        let email = String::from_utf8(Vec::<u8>::from(arr))
            .map_err(DeserializeError::FromUtf8Error)?
            .trim_matches(char::from(0))
            .to_string();

        Ok(Email(email))
    }
}
impl std::ops::Deref for Email {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
#[derive(PartialEq, Clone)]
pub struct Row {
    id: Id,
    username: Username,
    email: Email,
}
impl Row {
    pub const ID_OFFSET: usize = 0;
    pub const ID_RANGE: Range<usize> = Row::ID_OFFSET..(Row::ID_OFFSET + Id::MAX_SIZE);

    pub const USERNAME_OFFSET: usize = Self::ID_OFFSET + Id::MAX_SIZE;
    pub const USERNAME_RANGE: Range<usize> =
        Row::USERNAME_OFFSET..(Row::USERNAME_OFFSET + Username::MAX_SIZE);

    pub const EMAIL_OFFSET: usize = Self::USERNAME_OFFSET + Username::MAX_SIZE;
    pub const EMAIL_RANGE: Range<usize> = Row::EMAIL_OFFSET..(Row::EMAIL_OFFSET + Email::MAX_SIZE);

    pub const MAX_SIZE: usize = Id::MAX_SIZE + Username::MAX_SIZE + Email::MAX_SIZE;

    pub fn new(id: Id, username: Username, email: Email) -> Self {
        Self {
            id,
            username,
            email,
        }
    }
}
impl std::convert::From<Row> for [u8; Row::MAX_SIZE] {
    fn from(row: Row) -> [u8; Row::MAX_SIZE] {
        let Row {
            id,
            username,
            email,
        } = row;

        let mut bytes = [0; Row::MAX_SIZE];
        bytes[Row::ID_RANGE].copy_from_slice(&<[u8; Id::MAX_SIZE]>::from(id));
        bytes[Row::USERNAME_RANGE].copy_from_slice(&<[u8; Username::MAX_SIZE]>::from(username));
        bytes[Row::EMAIL_RANGE].copy_from_slice(&<[u8; Email::MAX_SIZE]>::from(email));
        bytes
    }
}
impl std::convert::TryFrom<&[u8]> for Row {
    type Error = DeserializeError;

    fn try_from(arr: &[u8]) -> Result<Self, Self::Error> {
        if arr.len() < Self::MAX_SIZE {
            return Err(DeserializeError::InvalidBytesSlice(arr.len()));
        }

        // Les indexation sont valide grâce à la vérification au-dessus.

        let id_bytes: [u8; Id::MAX_SIZE] =
            arr[Self::ID_RANGE]
                .try_into()
                .map_err(|_| DeserializeError::TryFromSliceError {
                    name: "id".to_owned(),
                    expected_size: Username::MAX_SIZE,
                    obtained_size: arr[Self::ID_RANGE].len(),
                })?;
        let id = Id::from(id_bytes);

        let username_bytes: [u8; Username::MAX_SIZE] = arr[Self::USERNAME_RANGE]
            .try_into()
            .map_err(|_| DeserializeError::TryFromSliceError {
                name: "username".to_owned(),
                expected_size: Username::MAX_SIZE,
                obtained_size: arr[Self::USERNAME_RANGE].len(),
            })?;
        let username = Username::try_from(username_bytes)?;

        let email_bytes: [u8; Email::MAX_SIZE] =
            arr[Self::EMAIL_RANGE]
                .try_into()
                .map_err(|_| DeserializeError::TryFromSliceError {
                    name: "email".to_owned(),
                    expected_size: Username::MAX_SIZE,
                    obtained_size: arr[Self::EMAIL_RANGE].len(),
                })?;
        let email = Email::try_from(email_bytes)?;

        Ok(Self {
            id,
            username,
            email,
        })
    }
}
impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {}, {})", *self.id, *self.username, *self.email)
    }
}

#[cfg(test)]
mod row_test {
    use super::*;

    #[test]
    fn test_id_from_into_u8_array() {
        let id_arr = <[u8; Id::MAX_SIZE]>::from(Id(42));
        assert_eq!(id_arr, [0, 0, 0, 0, 0, 0, 0, 42]);
        assert_eq!(Id::from(id_arr), Id(42));

        let id_arr = <[u8; Id::MAX_SIZE]>::from(Id(usize::MIN));
        assert_eq!(id_arr, [0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(Id::from(id_arr), Id(usize::MIN));

        let id_arr = <[u8; Id::MAX_SIZE]>::from(Id(usize::MAX));
        assert_eq!(id_arr, [255, 255, 255, 255, 255, 255, 255, 255]);
        assert_eq!(Id::from(id_arr), Id(usize::MAX));
    }

    #[test]
    fn test_username_from_into_u8_array() {
        let username = Username("abigaël".to_owned());
        let username_array = <[u8; Username::MAX_SIZE]>::from(username.clone());
        assert_eq!(
            username_array[..username.len()],
            [97, 98, 105, 103, 97, 195, 171, 108]
        );

        let username_deser =
            Username::try_from(<[u8; Username::MAX_SIZE]>::try_from(username_array).unwrap())
                .unwrap();
        assert_eq!(username_deser, username);
    }

    #[test]
    fn test_email_from_into_u8_array() {
        let email = Email("abigaël@yahoo.com".to_owned());
        let email_bytes = <[u8; Email::MAX_SIZE]>::from(email.clone());
        assert_eq!(
            email_bytes[..email.len()],
            [
                97, 98, 105, 103, 97, 195, 171, 108, 64, 121, 97, 104, 111, 111, 46, 99, 111, 109
            ]
        );

        let email_deser =
            Email::try_from(<[u8; Email::MAX_SIZE]>::try_from(email_bytes).unwrap()).unwrap();
        assert_eq!(email_deser, email);
    }

    #[test]
    fn test_row_from_into_u8_slice() {
        let id = Id(42);
        let username = Username("abigaël".to_string());
        let email = Email("abigaël@yahoo.com".to_string());

        let row = Row {
            id: id.clone(),
            username: username.clone(),
            email: email.clone(),
        };

        let arr = <[u8; Row::MAX_SIZE]>::from(row);

        assert_eq!(&arr[Row::ID_RANGE], &id.to_be_bytes());
        assert_eq!(
            &arr[Row::USERNAME_OFFSET..Row::USERNAME_OFFSET + username.len()],
            username.as_bytes()
        );
        assert_eq!(
            &arr[Row::EMAIL_OFFSET..Row::EMAIL_OFFSET + email.len()],
            email.as_bytes()
        );

        let Row {
            id: id_deser,
            username: username_deser,
            email: email_deser,
        } = Row::try_from(&arr[..]).unwrap();

        assert_eq!(id_deser, id);
        assert_eq!(username_deser, username);
        assert_eq!(email_deser, email);
    }
}
