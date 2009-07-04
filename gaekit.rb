require 'java'

module GAEKit
  module Datastore
    KIND = self.name

    module API
      import com.google.appengine.api.datastore.DatastoreServiceFactory
      import com.google.appengine.api.datastore.KeyFactory
      import com.google.appengine.api.datastore.Entity
      import com.google.appengine.api.datastore.Query
      import com.google.appengine.api.datastore.Text

      def service
        @service ||= DatastoreServiceFactory.datastore_service
      end
      module_function :service

      def to_java(object)
        if object.is_a?(String) && object.length >= 500
          Text.new(object)
        else
          object
        end
      end
      module_function :to_java

      def to_ruby(object)
        if object.is_a?(Java::JavaUtil::ArrayList)
          object.to_a
        elsif object.is_a?(Text)
          object.value
        else
          object
        end
      end
      module_function :to_ruby
    end

    module Methods
      def get(key)
        API.to_ruby((@entity ||= entity).get_property(key.to_s))
      end
      alias :[] :get

      def put(key, value)
        (@entity ||= entity).set_property(key.to_s, API.to_java(value))
        API.service.put(@entity)
        value
      end
      alias :[]= :put

      def has_key?(key)
        (@entity ||= entity).has_property(key.to_s)
      end
      alias :include? :has_key?

      def delete(key)
        (@entity ||= entity).remove_property(key.to_s)
        API.service.put(@entity)
        nil
      end

      def keys
        (@entity ||= entity).properties.map { |key, value| key }
      end

      def values
        (@entity ||= entity).properties.map { |key, value| API.to_ruby(value) }
      end

    private
      def entity
        API.service.get(API::KeyFactory.create_key(KIND, self.name))
      rescue NativeException
        new_entity = API::Entity.new(KIND, self.name)
        API.service.put(new_entity)
        new_entity
      end
    end

    def dump
      entities = {}
      API.service.prepare(API::Query.new(KIND)).as_iterator.each do |entity|
        properties = {}
        entity.properties.each do |key, value|
          properties[key] = API.to_ruby(value)
        end
        entities[entity.key.name] = properties
      end
      entities
    end
    module_function :dump
  end

  class Store
    extend Datastore::Methods
  end
end
