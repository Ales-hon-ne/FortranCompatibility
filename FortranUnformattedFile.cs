using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;

namespace FortranCompatibility
{
    /// <summary>
    /// Исключение, которое выбрасывается, если файл имеет неверную структуру.
    /// </summary>
    [Serializable]
    public sealed class FUFInvalidStructureException: ApplicationException
    {
        private static readonly object recordIndicator = "Record";
        private static readonly object positionIndicator = "Position";
        /// <summary>
        /// Номер логической записи, при чтении которой произошла ошибка.
        /// </summary>
        /// <value>
        /// Номер логической записи, если ошибка произошла при чтении конкретной записи,
        /// или null, если ошибка не связана с записью.
        /// </value>
        /// <remarks>
        /// Записи нумеруются с 0. Необходимо учесть, что в Fortran'е нумерация 
        /// записей ведётся с 1.
        /// </remarks>
        public int? Record
        {
            get
            {
                if (Data.Contains(recordIndicator))
                    return (int?)Data[recordIndicator];
                return null;
            }
            internal set
            {
                if (value.HasValue)
                    Data[recordIndicator] = value.Value;
                else if (Data.Contains(recordIndicator))
                    Data.Remove(recordIndicator);
            }
        }
        /// <summary>
        /// Смещение в источнике данных, по которому обнаружено нарушение структуры.
        /// </summary>
        /// <value>
        /// Число байт от начальной позиции до той, на которой была обнаружена ошибка структуры,
        /// или null, если данные отсутствуют.
        /// </value>
        public long? Position
        {
            get
            {
                if (Data.Contains(positionIndicator))
                    return (long?)Data[positionIndicator];
                return null;
            }
            internal set
            {
                if (value.HasValue)
                    Data[positionIndicator] = value.Value;
                else if (Data.Contains(positionIndicator))
                    Data.Remove(positionIndicator);
            }
        }
        private FUFInvalidStructureException() : this("Invalid exception object") { }
        internal FUFInvalidStructureException(string message) : base(message) { }
        internal FUFInvalidStructureException(string message, int record) : this(message)
        {
            Data.Add(recordIndicator, record);
        }
        internal FUFInvalidStructureException(string message,
            Exception innerException) : base(message, innerException)
        { }
        private FUFInvalidStructureException(System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context) : base(info, context)
        { }
    }

    /// <summary>
    /// Исключение, которое выбрасывается, если есть проблемы с источником данных.
    /// </summary>
    [Serializable]
    public sealed class FUFInvalidSourceException: ApplicationException
    {
        internal static readonly object sourceIndicator = "Source";
        private FUFInvalidSourceException() : this("Invalid exception object") { }
        internal FUFInvalidSourceException(string message) : base(message) { }
        internal FUFInvalidSourceException(string message, Exception innerException) : base(message, innerException) { }
        private FUFInvalidSourceException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }

    /// <summary>
    /// Класс обеспечивает чтение данных типа Fortran Unformatted Sequential.
    /// </summary>
    /// <remarks>
    /// Все данные из файла единожды считываются в конструкторе, после чего 
    /// хранятся в оперативной памяти.
    /// </remarks>
    // Структура файла.
    // Короткие записи.
    // Структура физической записи (fr): 
    //     byte len
    //     byte[count = min(len, 128)] data
    //     byte len
    // Если len = 129, то длина физической записи 128, а текущая логическая запись
    // включает в себя следующую физическую запись. Значения len > 129 недопустимы.
    // Структура логической записи (lr):
    //     fr(len = 129)[count >= 0]
    //     fr(len <= 128)
    // Структура файла:
    //     byte 75
    //     lr[count >= 0]
    //     byte 130
    // Длинные записи.
    // Структура физической записи (fr):
    //     int32 len
    //     byte[len] data
    //     int32 len
    // Логическая запись совпадает с физической.
    // Структура файла:
    //     fr[count >= 0]
    public sealed class FortranUnformattedFile: IEnumerable<Stream>
    {
        private readonly List<byte[]> rdata;
        /// <summary>
        /// Загружает данные из потока (длинные физические записи).
        /// </summary>
        /// <param name="data">Поток с данными</param>
        /// <exception cref="ArgumentNullException"/>
        /// <exception cref="FUFInvalidSourceException"/>
        /// <exception cref="FUFInvalidStructureException"/>
        /// <exception cref="ObjectDisposedException"/>
        private void LoadDataFromStreamLongRec(Stream data)
        {
            if (!data.CanSeek)
                throw new FUFInvalidSourceException("Источник данных не поддерживает поиск.");
            if (!data.CanRead)
                throw new FUFInvalidSourceException("Источник данных не поддерживает чтение.");

            data.Seek(0, SeekOrigin.Begin);

            var rd = new BinaryReader(data, Encoding.ASCII, true);

            while (data.Position < data.Length - 1)
            {
                if (data.Length - data.Position <= 4)
                    throw new FUFInvalidStructureException("Неожиданный конец файла.");
                var bmark = rd.ReadInt32();
                if (bmark < 0)
                    throw new FUFInvalidStructureException($"Запись {rdata.Count} имеет неверную структуру: маркер начала записи вне диапазона допусимых значений.", rdata.Count);
                if (bmark + 4 > data.Length - data.Position)
                    throw new FUFInvalidStructureException("Неожиданный конец файла.");
                var buf = rd.ReadBytes(bmark);
                rdata.Add(buf);
                var emark = rd.ReadInt32();
                if (emark != bmark)
                    throw new FUFInvalidStructureException($"Запись {rdata.Count - 1} имеет неверную структуру: маркер конца записи не соответствует маркеру начала записи.", rdata.Count - 1);
            }
            LongRecords = true;
        }
        /// <summary>
        /// Загружает данные из потока (короткие физические записи).
        /// </summary>
        /// <param name="data">Поток с данными</param>
        /// <exception cref="ArgumentNullException"/>
        /// <exception cref="FUFInvalidSourceException"/>
        /// <exception cref="FUFInvalidStructureException"/>
        /// <exception cref="ObjectDisposedException"/>
        private void LoadDataFromStreamShortRec(Stream data)
        {
            const int BOFMark = 0x4B; // маркер начала данных
            const int EOFMark = 0x82; // маркер конца данных
            const int maxRecLen = 0x80; // максимальная длина физической записи
            const int longRecMark = 0x81; // маркер длинной записи
            int bmark, emark;

            if (!data.CanRead)
                throw new FUFInvalidSourceException("Источник данных не поддерживает чтение.");
            if (!data.CanSeek)
                throw new FUFInvalidSourceException("Источник данных не поддерживает поиск.");

            data.Seek(0, SeekOrigin.Begin);
            bmark = data.ReadByte();

            if (bmark == -1)
            {
                throw new FUFInvalidSourceException("Источник данных пуст.");
            }

            labelReadLR:
            if (bmark != BOFMark)
            {
                LoadDataFromStreamLongRec(data);
                return;
            }

            var buf = new byte[maxRecLen];
            var td = new List<byte>(maxRecLen);
            while (true)
            {
                bmark = data.ReadByte();
                if (bmark == EOFMark) break;
                if (bmark > longRecMark)
                {
                    bmark = 0;
                    goto labelReadLR;
                }
                if (bmark == -1 || bmark > (data.Length - data.Position))
                {
                    bmark = 0;
                    goto labelReadLR;
                }
                data.Read(buf, 0, Math.Min(bmark, maxRecLen));
                emark = data.ReadByte();
                if (emark == -1)
                {
                    bmark = 0;
                    goto labelReadLR;
                }
                if (emark != bmark)
                {
                    bmark = 0;
                    goto labelReadLR;
                }
                td.AddRange(buf.Take(Math.Min(bmark, maxRecLen)));
                if (bmark != longRecMark)
                {
                    rdata.Add(td.ToArray());
                    td.Clear();
                }
            }
            if (data.ReadByte() != -1)
                throw new FUFInvalidStructureException("Есть данные после маркера конца данных.");
            LongRecords = false;
        }
        private FortranUnformattedFile()
        {
            rdata = new List<byte[]>();
            LongRecords = false;
        }
        /// <summary>
        /// Инициализирует новый экземпляр класса, заполненный данными из указанного потока. 
        /// </summary>
        /// <param name="data">Поток с данными типа Fortran Unformatted</param>
        /// <exception cref="ArgumentNullException">
        /// Выбрасывается, если <paramref name="data"/> имеет значение null.
        /// </exception>
        /// <exception cref="FUFInvalidSourceException">
        /// Выбрасывается, если произошла ошибка при чтении файла.
        /// </exception>
        /// <exception cref="FUFInvalidStructureException">
        /// Выбрасывается, если структура данных недопустима.
        /// </exception>
        /// <exception cref="ObjectDisposedException">
        /// Выбрасывается, если объект <paramref name="data"/> был удалён.
        /// </exception>
        public FortranUnformattedFile(Stream data) : this()
        {
            try
            {
                if (data.CanSeek)
                    LoadDataFromStreamShortRec(data);
                else
                {
                    var mcache = new MemoryStream();
                    data.CopyTo(mcache);
                    LoadDataFromStreamShortRec(mcache);
                }
            }
            catch (Exception e)
            {
                e.Data.Add(FUFInvalidSourceException.sourceIndicator, "stream " + data.GetHashCode().ToString("x8"));
                throw;
            }
        }
        /// <summary>
        /// Инициализирует новый экземпляр класса, заполненный данными из указанного файла.
        /// </summary>
        /// <param name="path">Путь к файлу типа Fortran Unformatted</param>
        /// <exception cref="FileNotFoundException">
        /// Выбрасывается, если искомый файл не существует или ОС запретила доступ к файлу.
        /// </exception>
        /// <exception cref="FUFInvalidSourceException">
        /// Выбрасывается, если произошла ошибка при чтении файла.
        /// </exception>
        /// <exception cref="FUFInvalidStructureException">
        /// Выбрасывается, если структура данных недопустима.
        /// </exception>
        public FortranUnformattedFile(string path) : this()
        {
            if (!File.Exists(path))
                throw new FileNotFoundException("Не удалось получить доступ к файлу.", path ?? "<null>");
            try
            {
                using (var fs = File.OpenRead(path))
                    LoadDataFromStreamShortRec(fs);
            }
            catch (NotSupportedException e)
            {
                var newe = new FUFInvalidSourceException(e.Message, e);
                newe.Data.Add(FUFInvalidSourceException.sourceIndicator, Path.GetFullPath(path));
                throw newe;
            }
            catch (Exception e)
            {
                e.Data.Add(FUFInvalidSourceException.sourceIndicator, Path.GetFullPath(path));
                throw;
            }
        }

        /// <summary>
        /// Возвращает содержимое указанной логической записи в виде потока данных.
        /// </summary>
        /// <param name="index">Номер записи (начиная с 0)</param>
        /// <returns>
        /// Поток, содержащий логическую запись с указанным номером, доступный только для чтения.
        /// </returns>
        /// <remarks>
        /// В Фортране записи нумеруются с 1, будьте внимательны.
        /// </remarks>
        public Stream this[int index] => new MemoryStream(rdata[index], false);
        /// <summary>
        /// Показывает, были ли использованы длинные записи.
        /// </summary>
        public bool LongRecords { get; private set; }
        /// <summary>
        /// Количество логических записей.
        /// </summary>
        public int Count => rdata.Count;

        #region IEnumerable realization
        IEnumerator<Stream> IEnumerable<Stream>.GetEnumerator() => 
            rdata.Select(val => new MemoryStream(val, false) as Stream).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => (this as IEnumerable<Stream>).GetEnumerator();
        #endregion
    }
}
