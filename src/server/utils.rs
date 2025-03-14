use arrow::{
    array::RecordBatch,
    error::ArrowError,
    ipc::writer::{self, IpcWriteOptions},
};
use arrow_flight::FlightData;

pub fn batches_to_flight_data_without_schema(
    batches: Vec<RecordBatch>,
) -> Result<Vec<FlightData>, ArrowError> {
    let options = IpcWriteOptions::default();
    let mut dictionaries = vec![];
    let mut flight_data = vec![];

    let data_gen = writer::IpcDataGenerator::default();
    let mut dictionary_tracker =
        writer::DictionaryTracker::new_with_preserve_dict_id(false, options.preserve_dict_id());

    for batch in batches.iter() {
        let (encoded_dictionaries, encoded_batch) =
            data_gen.encoded_batch(batch, &mut dictionary_tracker, &options)?;

        dictionaries.extend(encoded_dictionaries.into_iter().map(Into::into));
        flight_data.push(encoded_batch.into());
    }

    let mut stream = Vec::with_capacity(dictionaries.len() + flight_data.len());
    // No schema added here
    stream.extend(dictionaries);
    stream.extend(flight_data);

    Ok(stream)
}
